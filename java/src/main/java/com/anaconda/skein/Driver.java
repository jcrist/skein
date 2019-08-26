package com.anaconda.skein;

import com.google.common.base.Strings;
import com.google.common.collect.ObjectArrays;
import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class Driver {

  private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

  // One for the boss, one for the worker. Should be fine under normal loads.
  private static final int NUM_EVENT_LOOP_GROUP_THREADS = 2;

  // The thread bounds for handling requests. Since we use locking at some
  // level, we can only get so much parallelism in *handling* requests.
  private static final int MIN_GRPC_EXECUTOR_THREADS = 2;
  private static final int MAX_GRPC_EXECUTOR_THREADS = 10;

  // Owner rwx (700)
  private static final FsPermission SKEIN_DIR_PERM =
      FsPermission.createImmutable((short)448);
  // Owner rw (600)
  private static final FsPermission SKEIN_FILE_PERM =
      FsPermission.createImmutable((short)384);

  private Configuration conf;

  private boolean loggedIn = false;

  private UserGroupInformation ugi;
  private FileSystem defaultFileSystem;
  private YarnClient defaultYarnClient;
  private LogClient logClient;

  private String classpath;

  // Initialization arguments
  // -- environment variables
  private int callbackPort;
  private ByteString certBytes;
  private ByteString keyBytes;
  // -- commandline flags
  private String jarPath = null;
  private String keytabPath = null;
  private String principal = null;
  private boolean daemon = false;

  private Server server;

  private final Map<ApplicationId, List<StreamObserver<Msg.ApplicationReport>>> startedCallbacks =
      new HashMap<ApplicationId, List<StreamObserver<Msg.ApplicationReport>>>();

  private void startServer() throws IOException {
    // Setup and start the server
    SslContext sslContext = GrpcSslContexts
        .forServer(certBytes.newInput(), keyBytes.newInput())
        .trustManager(certBytes.newInput())
        .clientAuth(ClientAuth.REQUIRE)
        .sslProvider(SslProvider.OPENSSL)
        .build();

    NioEventLoopGroup eg = new NioEventLoopGroup(NUM_EVENT_LOOP_GROUP_THREADS);
    ThreadPoolExecutor executor = Utils.newThreadPoolExecutor(
        "grpc-executor",
        MIN_GRPC_EXECUTOR_THREADS,
        MAX_GRPC_EXECUTOR_THREADS,
        true);

    server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
        .sslContext(sslContext)
        .addService(new DriverImpl())
        .workerEventLoopGroup(eg)
        .bossEventLoopGroup(eg)
        .executor(executor)
        .build()
        .start();

    LOG.info("Driver started, listening on {}", server.getPort());

    Runtime.getRuntime().addShutdownHook(
        new Thread() {
          @Override
          public void run() {
            Driver.this.stopServer();
          }
        });
  }

  private void stopServer() {
    if (server != null) {
      server.shutdown();
      LOG.debug("Driver shut down");
    }
  }

  /** Main Entry Point. **/
  public static void main(String[] args) {
    LOG.debug("Starting Skein version {}", Utils.getSkeinVersion());

    // Maybe specify the netty native workdir. This is necessary for systems
    // where `/tmp` is not executable.
    Utils.configureNettyNativeWorkDir();

    try {
      Driver driver = new Driver();
      driver.init(args);
      driver.run();
      System.exit(0);
    } catch (Throwable exc) {
      LOG.error("Error running Driver", exc);
      System.exit(1);
    }
  }

  private void usageError() {
    LOG.error("Usage: COMMAND --jar PATH [--keytab PATH, --principal NAME] [--daemon]");
    System.exit(1);
  }

  private void init(String[] args) throws IOException {
    // Parse environment variables
    certBytes = ByteString.copyFromUtf8(System.getenv("SKEIN_CERTIFICATE"));
    if (certBytes == null) {
      LOG.error("Couldn't find 'SKEIN_CERTIFICATE' envar");
      System.exit(1);
    }
    keyBytes = ByteString.copyFromUtf8(System.getenv("SKEIN_KEY"));
    if (keyBytes == null) {
      LOG.error("Couldn't find 'SKEIN_KEY' envar");
      System.exit(1);
    }
    String callbackPortEnv = System.getenv("SKEIN_CALLBACK_PORT");
    if (callbackPortEnv == null) {
      LOG.error("Couldn't find 'SKEIN_CALLBACK_PORT' envar");
      System.exit(1);
    }
    callbackPort = Integer.valueOf(callbackPortEnv);

    // Parse arguments
    int i = 0;
    while (i < args.length) {
      String value = (i + 1 < args.length) ? args[i + 1] : null;
      switch (args[i]) {
        case "--jar":
          jarPath = value;
          i += 2;
          break;
        case "--keytab":
          keytabPath = value;
          i += 2;
          break;
        case "--principal":
          principal = value;
          i += 2;
          break;
        case "--daemon":
          daemon = true;
          i += 1;
          break;
        default:
          usageError();
          break;
      }
    }
    if (jarPath == null || ((keytabPath == null) != (principal == null))) {
      usageError();
    }

    // Login using the appropriate method. We don't need to start a thread to
    // do periodic logins, as we're only making use of normal Hadoop RPC apis,
    // and these automatically handle relogin on failure. See
    // https://stackoverflow.com/q/34616676/1667287
    if (keytabPath != null) {
      LOG.debug("Logging in using keytab: {}, principal: {}", keytabPath, principal);
      UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
      ugi = UserGroupInformation.getLoginUser();
    } else {
      LOG.debug("Logging in using ticket cache");
      ugi = UserGroupInformation.getLoginUser();
    }
    // If needed, ensure user has obtained a kerberos ticket, otherwise the
    // driver will lockup (missing kerberos tickets are logged, but no
    // exception is raised in the caller, which is unfortunate).
    // UserGroupInformation also caches and can't be reset, so the process must
    // be killed and restarted.  We keep the driver running (even though it
    // can't do anything) so that client processes can get a nice error
    // message, rather than having to look in the logs.
    if (UserGroupInformation.isSecurityEnabled()
        && !(ugi.hasKerberosCredentials() || ugi.getCredentials().numberOfTokens() > 0)) {
      LOG.warn("Kerberos ticket not found, please kinit and restart");
      loggedIn = false;
    } else {
      loggedIn = true;
    }

    conf = new YarnConfiguration();

    // Build the classpath for running the appmaster
    StringBuilder cpBuilder = new StringBuilder(Environment.CLASSPATH.$$());
    cpBuilder.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      cpBuilder.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      cpBuilder.append(c.trim());
    }
    classpath = cpBuilder.toString();
  }

  private void run() throws Exception {
    // Connect to hdfs as *this* user
    defaultFileSystem = getFs();
    // Start the yarn client as *this* user
    defaultYarnClient = getYarnClient();
    // Create a logs client
    logClient = new LogClient(conf);

    // Start the server
    startServer();

    // Report back the port we're listening on
    LOG.debug("Reporting gRPC server port back to the launching process");
    Socket callback = new Socket("127.0.0.1", callbackPort);
    DataOutputStream dos = new DataOutputStream(callback.getOutputStream());
    dos.writeInt(server.getPort());
    dos.close();
    callback.close();

    if (daemon) {
      server.awaitTermination();
    } else {
      // Wait until EOF or broken pipe from stdin
      while (System.in.read() != -1) {}
      LOG.debug("Starting process disconnected, shutting down");
    }
  }

  public FileSystem getFs() throws IOException {
    return FileSystem.get(conf);
  }

  public YarnClient getYarnClient() {
    YarnClient client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();
    return client;
  }

  public Path getAppDir(FileSystem fs, ApplicationId appId) {
    return new Path(fs.getHomeDirectory(), ".skein/" + appId.toString());
  }

  public Map<String, String> getApplicationLogs(
      final ApplicationId appId, final String owner, String user)
      throws IOException, InterruptedException {
    if (user.isEmpty()) {
      return logClient.getLogs(appId, owner);
    } else {
      return UserGroupInformation.createProxyUser(user, ugi).doAs(
          new PrivilegedExceptionAction<Map<String, String>>() {
            public Map<String, String> run() throws IOException {
              return logClient.getLogs(appId, owner);
            }
          });
    }
  }

  public void killApplication(final ApplicationId appId, String user)
      throws IOException, YarnException, InterruptedException {
    if (user.isEmpty()) {
      killApplicationInner(defaultYarnClient, defaultFileSystem, appId);
    } else {
      UserGroupInformation.createProxyUser(user, ugi).doAs(
          new PrivilegedExceptionAction<Void>() {
            public Void run() throws IOException, YarnException {
              killApplicationInner(getYarnClient(), getFs(), appId);
              return null;
            }
          });
    }
  }

  private void killApplicationInner(YarnClient yarnClient, FileSystem fs,
        ApplicationId appId) throws IOException, YarnException {

    LOG.debug("Killing application {}", appId);
    yarnClient.killApplication(appId);
    deleteAppDir(fs, getAppDir(fs, appId));
  }

  /** Start a new application. **/
  public ApplicationId submitApplication(final Model.ApplicationSpec spec)
      throws IOException, YarnException, InterruptedException {
    if (spec.getUser().isEmpty()) {
      return submitApplicationInner(defaultYarnClient, defaultFileSystem, spec);
    } else {
      return UserGroupInformation.createProxyUser(spec.getUser(), ugi).doAs(
        new PrivilegedExceptionAction<ApplicationId>() {
          public ApplicationId run() throws IOException, YarnException {
            return submitApplicationInner(getYarnClient(), getFs(), spec);
          }
        });
    }
  }

  private ApplicationId submitApplicationInner(YarnClient yarnClient,
      FileSystem fs, Model.ApplicationSpec spec) throws IOException, YarnException {
    // First validate the spec request
    spec.validate();

    // Get an application id. This is needed before doing anything else so we
    // can upload additional files to the application directory
    YarnClientApplication app = yarnClient.createApplication();
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    // Start building the appmaster request
    Model.Master master = spec.getMaster();

    // Directory to store temporary application resources
    Path appDir = getAppDir(fs, appId);

    // Setup the appmaster environment variables
    Map<String, String> env = new HashMap<String, String>();
    env.putAll(master.getEnv());
    env.put("CLASSPATH", classpath);
    env.put("SKEIN_APPLICATION_ID", appId.toString());
    String lang = System.getenv("LANG");
    if (lang != null) {
      env.put("LANG", lang);
    }

    // Setup the appmaster commands
    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    String log4jConfig = (master.hasLogConfig()
                          ? "-Dlog4j.configuration=file:./.skein.log4j.properties "
                          : "");
    Level logLevel = master.getLogLevel();
    List<String> commands = Arrays.asList(
        (Environment.JAVA_HOME.$$() + "/bin/java "
         + "-Xmx128M "
         + log4jConfig
         + "-Dskein.log.level=" + logLevel
         + " -Dskein.log.directory=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
         + " com.anaconda.skein.ApplicationMaster "
         + appDir
         + " >" + logdir + "/application.master.log 2>&1"));

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ByteBuffer fsTokens = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      fsTokens = collectTokens(yarnClient, fs, spec);
    } else {
      env.put("HADOOP_USER_NAME", ugi.getUserName());
    }

    Map<ApplicationAccessType, String> acls = spec.getAcls().getYarnAcls();

    try {
      // Setup the LocalResources for the appmaster and containers
      Map<String, LocalResource> localResources = setupAppDir(fs, spec, appDir);

      ContainerLaunchContext amContext = ContainerLaunchContext.newInstance(
          localResources, env, commands, null, fsTokens, acls);

      appContext.setApplicationType("skein");
      appContext.setAMContainerSpec(amContext);
      appContext.setApplicationName(spec.getName());
      appContext.setResource(master.getResources());
      appContext.setPriority(Priority.newInstance(0));
      appContext.setQueue(spec.getQueue());
      appContext.setMaxAppAttempts(spec.getMaxAttempts());
      appContext.setNodeLabelExpression(Strings.emptyToNull(spec.getNodeLabel()));
      appContext.setApplicationTags(spec.getTags());

      LOG.info("Submitting application...");
      yarnClient.submitApplication(appContext);
    } catch (Exception exc) {
      // Ensure application directory is deleted on submission failure
      deleteAppDir(fs, appDir);
      throw exc;
    }

    return appId;
  }

  private ByteBuffer collectTokens(YarnClient yarnClient, FileSystem fs,
      Model.ApplicationSpec spec) throws IOException, YarnException {
    // Collect security tokens as needed
    LOG.debug("Collecting filesystem delegation tokens");
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    TokenCache.obtainTokensForNamenodes(
            credentials,
            ObjectArrays.concat(
                    new Path(fs.getUri()),
                    spec.getFileSystems().toArray(new Path[0])),
            conf);

    boolean hasRMToken = false;
    for (Token<?> token: credentials.getAllTokens()) {
      if (token.getKind().equals(RMDelegationTokenIdentifier.KIND_NAME)) {
        LOG.debug("RM delegation token already acquired");
        hasRMToken = true;
        break;
      }
    }
    if (!hasRMToken) {
      LOG.debug("Adding RM delegation token");
      Text rmDelegationTokenService = ClientRMProxy.getRMDelegationTokenService(conf);
      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
      org.apache.hadoop.yarn.api.records.Token rmDelegationToken =
          yarnClient.getRMDelegationToken(new Text(tokenRenewer));
      Token<TokenIdentifier> rmToken = ConverterUtils.convertFromYarn(
          rmDelegationToken, rmDelegationTokenService
      );
      credentials.addToken(rmDelegationTokenService, rmToken);
    }

    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    return ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
  }

  private LocalResource finalizeSecurityFile(
      FileSystem fs, Map<Path, Path> uploadCache, Path appDir,
      LocalResource file, ByteString bytes, String filename)
      throws IOException {
    if (file != null) {
      finalizeLocalResource(uploadCache, appDir, file, false);
    } else {
      Path uploadPath = new Path(appDir, filename);
      OutputStream out = fs.create(uploadPath);
      try {
        bytes.writeTo(out);
      } finally {
        out.close();
      }
      file = Utils.localResource(fs, uploadPath, LocalResourceType.FILE);
    }
    return file;
  }

  private void deleteAppDir(FileSystem fs, Path appDir) {
    try {
      if (fs.exists(appDir)) {
        if (fs.delete(appDir, true)) {
          LOG.debug("Deleted application directory {}", appDir);
          return;
        }
        if (fs.exists(appDir)) {
          LOG.warn("Failed to delete application directory {}", appDir);
        }
      }
    } catch (IOException exc) {
      LOG.warn("Failed to delete application directory {}", appDir, exc);
    }
  }

  private Map<String, LocalResource> setupAppDir(FileSystem fs,
        Model.ApplicationSpec spec, Path appDir) throws IOException {

    // Make the ~/.skein/app_id dir
    LOG.info("Uploading application resources to {}", appDir);
    FileSystem.mkdirs(fs, appDir, SKEIN_DIR_PERM);

    Map<Path, Path> uploadCache = new HashMap<Path, Path>();

    // Create LocalResources for the crt/pem files, and add them to the
    // security object.
    Model.Master master = spec.getMaster();
    Model.Security security = master.getSecurity();
    if (security == null) {
      security = new Model.Security();
      security.setCertBytes(certBytes);
      security.setKeyBytes(keyBytes);
      master.setSecurity(security);
    }
    LocalResource certFile = finalizeSecurityFile(
        fs, uploadCache, appDir, security.getCertFile(), security.getCertBytes(),
        ".skein.crt");
    LocalResource keyFile = finalizeSecurityFile(
        fs, uploadCache, appDir, security.getKeyFile(), security.getKeyBytes(),
        ".skein.pem");
    security.setCertFile(certFile);
    security.setKeyFile(keyFile);

    // Setup the LocalResources for the services
    for (Map.Entry<String, Model.Service> entry: spec.getServices().entrySet()) {
      finalizeService(entry.getKey(), entry.getValue(), fs,
                      uploadCache, appDir, certFile, keyFile);
    }
    spec.validate();

    // Setup the LocalResources for the application master
    Map<String, LocalResource> lr = master.getLocalResources();
    for (LocalResource resource : lr.values()) {
      finalizeLocalResource(uploadCache, appDir, resource, true);
    }
    lr.put(".skein.jar", newLocalResource(uploadCache, appDir, jarPath));
    if (master.hasLogConfig()) {
      LocalResource logConfig = master.getLogConfig();
      finalizeLocalResource(uploadCache, appDir, logConfig, false);
      lr.put(".skein.log4j.properties", logConfig);
    }
    lr.put(".skein.crt", certFile);
    lr.put(".skein.pem", keyFile);

    // Write the application specification to file
    Path specPath = new Path(appDir, ".skein.proto");
    LOG.debug("Writing application specification to {}", specPath);
    OutputStream out = fs.create(specPath);
    try {
      MsgUtils.writeApplicationSpec(spec).writeTo(out);
    } finally {
      out.close();
    }
    LocalResource specFile = Utils.localResource(fs, specPath,
                                                 LocalResourceType.FILE);
    lr.put(".skein.proto", specFile);
    return lr;
  }

  private void finalizeService(String serviceName, Model.Service service,
      FileSystem fs, Map<Path, Path> uploadCache, Path appDir,
      LocalResource certFile, LocalResource keyFile) throws IOException {

    // Write the service script to file
    final Path scriptPath = new Path(appDir, serviceName + ".sh");
    LOG.debug("Writing script for service '{}' to {}", serviceName, scriptPath);
    Utils.stringToFile(service.getScript(), fs.create(scriptPath));
    LocalResource scriptFile = Utils.localResource(fs, scriptPath,
                                                   LocalResourceType.FILE);

    // Build command to execute script and set as new script
    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    service.setScript("bash .skein.sh >" + logdir + "/" + serviceName + ".log 2>&1");

    // Upload files/archives as necessary
    Map<String, LocalResource> lr = service.getLocalResources();
    for (LocalResource resource : lr.values()) {
      finalizeLocalResource(uploadCache, appDir, resource, true);
    }

    // Add script/crt/pem files
    lr.put(".skein.sh", scriptFile);
    lr.put(".skein.crt", certFile);
    lr.put(".skein.pem", keyFile);

    // Add LANG if present
    String lang = System.getenv("LANG");
    if (lang != null) {
      service.getEnv().put("LANG", lang);
    }
  }

  private LocalResource newLocalResource(Map<Path, Path> uploadCache, Path appDir,
      String localPath) throws IOException {
    LocalResource out = LocalResource.newInstance(
        URL.newInstance("file", null, -1, localPath),
        LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION,
        0, 0);
    finalizeLocalResource(uploadCache, appDir, out, false);
    return out;
  }

  private void finalizeLocalResource(Map<Path, Path> uploadCache,
      Path appDir, LocalResource file, boolean hash) throws IOException {

    Path srcPath = Utils.pathFromUrl(file.getResource());
    Path dstPath;

    FileSystem dstFs = appDir.getFileSystem(conf);
    FileSystem srcFs = srcPath.getFileSystem(conf);

    dstPath = uploadCache.get(srcPath);
    if (dstPath == null) {
      if (Utils.equalFs(srcFs, dstFs)) {
        // File exists in filesystem but not in upload cache
        dstPath = srcPath;
      } else {
        // File needs to be uploaded to the destination filesystem
        MessageDigest md;
        try {
          md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
          throw new IllegalArgumentException("MD5 not supported on this platform");
        }
        if (hash) {
          md.update(srcPath.toString().getBytes());
          String prefix = Utils.hexEncode(md.digest());
          dstPath = new Path(new Path(appDir, prefix), srcPath.getName());
        } else {
          dstPath = new Path(appDir, srcPath.getName());
        }
        LOG.debug("Uploading {} to {}", srcPath, dstPath);
        FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, conf);
        dstFs.setPermission(dstPath, SKEIN_FILE_PERM);
      }
      uploadCache.put(srcPath, dstPath);
    }

    file.setResource(ConverterUtils.getYarnUrlFromPath(dstPath));

    FileStatus status = dstFs.getFileStatus(dstPath);

    // Only set size & timestamp if not set already
    if (file.getSize() == 0) {
      file.setSize(status.getLen());
    }

    if (file.getTimestamp() == 0) {
      file.setTimestamp(status.getModificationTime());
    }
  }

  private boolean hasStarted(ApplicationReport report) {
    switch (report.getYarnApplicationState()) {
      case RUNNING:
      case FINISHED:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
    }
  }

  private boolean hasCompleted(ApplicationReport report) {
    switch (report.getYarnApplicationState()) {
      case FINISHED:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
    }
  }

  private synchronized void addWatcher(final ApplicationId appId,
      StreamObserver<Msg.ApplicationReport> resp) {

    LOG.debug("New watcher callback requested for application {}", appId);

    if (startedCallbacks.get(appId) != null) {
      LOG.debug("Watcher for {} already exists, adding stream to callback", appId);
      startedCallbacks.get(appId).add(resp);
      return;
    }
    LOG.debug("No watcher exists for {}, creating one", appId);

    final List<StreamObserver<Msg.ApplicationReport>> callbacks =
        new ArrayList<StreamObserver<Msg.ApplicationReport>>();
    startedCallbacks.put(appId, callbacks);
    callbacks.add(resp);

    Thread watcher = new Thread() {
        @Override
        public void run() {
          Thread thisThread = Thread.currentThread();
          ApplicationReport report = null;
          while (!thisThread.isInterrupted()) {
            try {
              // Get report
              report = defaultYarnClient.getApplicationReport(appId);
            } catch (Exception exc) {
              LOG.warn("Failed to get report for {}. Notifying {} callbacks.",
                       appId, callbacks.size());
              for (StreamObserver<Msg.ApplicationReport> resp : callbacks) {
                // Send error
                try {
                  resp.onError(Status.INTERNAL
                      .withDescription("Failed to get applications, exception:\n"
                                       + exc.getMessage())
                      .asRuntimeException());
                } catch (StatusRuntimeException cbExc) {
                  if (cbExc.getStatus().getCode() != Status.Code.CANCELLED) {
                    LOG.warn("Callback failed for app_id: {}, status: {}",
                             appId, cbExc.getStatus());
                  }
                }
              }
              break;
            }

            if (hasStarted(report)) {
              LOG.debug("Notifying that {} has started. {} callbacks registered.",
                        appId, callbacks.size());
              for (StreamObserver<Msg.ApplicationReport> resp : callbacks) {
                // Send report
                try {
                  resp.onNext(MsgUtils.writeApplicationReport(report));
                  resp.onCompleted();
                } catch (StatusRuntimeException cbExc) {
                  if (cbExc.getStatus().getCode() != Status.Code.CANCELLED) {
                    LOG.warn("Callback failed for app_id: {}, status: {}",
                             appId, cbExc.getStatus());
                  }
                }
              }
              break;
            }

            LOG.trace("Waiting for application {} to start", appId);

            // Sleep for 1 second
            try {
              thisThread.sleep(1000);
            } catch (InterruptedException exc) { }
          }

          // Remove callbacks for this appId
          LOG.debug("Removing callbacks for {}.", appId);
          synchronized (Driver.this) {
            startedCallbacks.remove(appId);
          }
        }
      };
    watcher.setDaemon(true);
    watcher.start();
  }

  class DriverImpl extends DriverGrpc.DriverImplBase {

    public boolean notLoggedIn(StreamObserver<?> resp) {
      if (!loggedIn) {
        resp.onError(Status.UNAUTHENTICATED
            .withDescription("Kerberos ticket not found, please kinit and restart")
            .asRuntimeException());
        return true;
      }
      return false;
    }

    @Override
    public void ping(Msg.Empty req, StreamObserver<Msg.Empty> resp) {
      resp.onNext(MsgUtils.EMPTY);
      resp.onCompleted();
    }

    @Override
    public void getApplications(Msg.ApplicationsRequest req,
        StreamObserver<Msg.ApplicationsResponse> resp) {

      if (notLoggedIn(resp)) {
        return;
      }

      EnumSet<YarnApplicationState> states;
      if (req.getStatesCount() == 0) {
        states = EnumSet.of(YarnApplicationState.SUBMITTED,
                            YarnApplicationState.ACCEPTED,
                            YarnApplicationState.RUNNING);
      } else {
        states = EnumSet.noneOf(YarnApplicationState.class);
        for (Msg.ApplicationState.Type s : req.getStatesList()) {
          states.add(MsgUtils.readApplicationState(s));
        }
      }

      List<ApplicationReport> reports;
      try {
        reports = defaultYarnClient.getApplications(
          new HashSet<String>(Arrays.asList("skein")), states);
      } catch (Exception exc) {
        resp.onError(Status.INTERNAL
            .withDescription("Failed to get applications, exception:\n"
                            + exc.getMessage())
            .asRuntimeException());
        return;
      }

      // XXX: Much like the Queue API, YARN actually provides a method to have
      // the server do this filtering in the query, but then doesn't expose it
      // in `YarnClient`. Newer versions of YARN support querying all but the
      // ranges in the YarnClient API, we should use these when available.
      String name = Strings.emptyToNull(req.getName());
      String user = Strings.emptyToNull(req.getUser());
      String queue = Strings.emptyToNull(req.getQueue());
      long startedBegin = req.getStartedBegin();
      long startedEnd = req.getStartedEnd();
      if (startedEnd == 0) {
        startedEnd = Long.MAX_VALUE;
      }
      long finishedBegin = req.getFinishedBegin();
      long finishedEnd = req.getFinishedEnd();
      if (finishedEnd == 0) {
        finishedEnd = Long.MAX_VALUE;
      }

      Msg.ApplicationsResponse.Builder builder = Msg.ApplicationsResponse.newBuilder();
      for (ApplicationReport report : reports) {
        if (name != null && !report.getName().equals(name)) {
          continue;
        }
        if (user != null && !report.getUser().equals(user)) {
          continue;
        }
        if (queue != null && !report.getQueue().equals(queue)) {
          continue;
        }
        // Ranges are inclusive on both ends
        long startTime = report.getStartTime();
        if (startedBegin > startTime || startedEnd < startTime) {
          continue;
        }
        long finishTime = report.getFinishTime();
        if (finishedBegin > finishTime || finishedEnd < finishTime) {
          continue;
        }
        builder.addReports(MsgUtils.writeApplicationReport(report));
      }

      resp.onNext(builder.build());
      resp.onCompleted();
    }

    @Override
    public void getNodes(Msg.NodesRequest req,
        StreamObserver<Msg.NodesResponse> resp) {

      if (notLoggedIn(resp)) {
        return;
      }

      EnumSet<NodeState> states;
      states = EnumSet.noneOf(NodeState.class);
      for (Msg.NodeState.Type s : req.getStatesList()) {
        try {
          states.add(MsgUtils.readNodeState(s));
        } catch (IllegalArgumentException exc) {
          // NodeState not supported by this version of Hadoop, ignore
          continue;
        }
      }

      List<NodeReport> reports;
      if (states.isEmpty() && req.getStatesCount() != 0) {
        // There were states requested, but none of them exist for this version
        // of Hadoop. Output empty list, no nodes with those states exist.
        reports = new ArrayList<NodeReport>();
      } else {
        try {
          reports = defaultYarnClient.getNodeReports(states.toArray(new NodeState[0]));
        } catch (Exception exc) {
          resp.onError(Status.INTERNAL
              .withDescription("Failed to get node reports, exception:\n"
                              + exc.getMessage())
              .asRuntimeException());
          return;
        }
      }

      resp.onNext(MsgUtils.writeNodesResponse(reports));
      resp.onCompleted();
    }

    @Override
    public void getQueue(Msg.QueueRequest req, StreamObserver<Msg.Queue> resp) {
      if (notLoggedIn(resp)) {
        return;
      }

      String name = req.getName();

      QueueInfo queue;
      try {
        queue = defaultYarnClient.getQueueInfo(name);
      } catch (Exception exc) {
        resp.onError(Status.INTERNAL
            .withDescription("Failed to get queue information, exception:\n"
                             + exc.getMessage())
            .asRuntimeException());
        return;
      }

      if (queue == null) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Queue '" + name + "' does not exist")
            .asRuntimeException());
        return;
      }

      resp.onNext(MsgUtils.writeQueue(queue));
      resp.onCompleted();
    }

    @Override
    public void getChildQueues(Msg.QueueRequest req, StreamObserver<Msg.QueuesResponse> resp) {
      if (notLoggedIn(resp)) {
        return;
      }

      String name = req.getName();

      List<QueueInfo> queues;
      try {
        queues = defaultYarnClient.getChildQueueInfos(name);
      } catch (NullPointerException exc) {
        // This is dumb. The YARN ResourceManager server exposes a flexible
        // queue querying API, but YarnClient then wraps it in a way that
        // removes much of the flexibility (e.g. there's no way to recursively
        // get child queues, even though the server does support that). Rather
        // than reimplement the client here (either requires opening a new
        // connection or reimplementing all client requests), we hack around
        // the exposed API.
        //
        // Here we'd like to get a list of child queues in a single server
        // request. Unfortunately, the exposed method doesn't properly handle
        // null pointers, so we catch the NullPointerException here.
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Queue '" + name + "' does not exist")
            .asRuntimeException());
        return;
      } catch (Exception exc) {
        resp.onError(Status.INTERNAL
            .withDescription("Failed to get queue information, exception:\n"
                             + exc.getMessage())
            .asRuntimeException());
        return;
      }

      Msg.QueuesResponse.Builder builder = Msg.QueuesResponse.newBuilder();
      for (QueueInfo queue : queues) {
        builder.addQueues(MsgUtils.writeQueue(queue));
      }
      resp.onNext(builder.build());
      resp.onCompleted();
    }

    @Override
    public void getAllQueues(Msg.Empty req, StreamObserver<Msg.QueuesResponse> resp) {
      if (notLoggedIn(resp)) {
        return;
      }

      List<QueueInfo> queues;
      try {
        queues = defaultYarnClient.getAllQueues();
      } catch (Exception exc) {
        resp.onError(Status.INTERNAL
            .withDescription("Failed to get queue information, exception:\n"
                             + exc.getMessage())
            .asRuntimeException());
        return;
      }

      Msg.QueuesResponse.Builder builder = Msg.QueuesResponse.newBuilder();
      for (QueueInfo queue : queues) {
        builder.addQueues(MsgUtils.writeQueue(queue));
      }
      resp.onNext(builder.build());
      resp.onCompleted();
    }

    private ApplicationReport getReport(String appIdString,
        StreamObserver<?> resp) {

      ApplicationId appId = Utils.appIdFromString(appIdString);

      if (appId == null) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Invalid ApplicationId '" + appIdString + "'")
            .asRuntimeException());
        return null;
      }

      ApplicationReport report;
      try {
        report = defaultYarnClient.getApplicationReport(appId);
      } catch (Exception exc) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Unknown ApplicationId '" + appIdString + "'")
            .asRuntimeException());
        return null;
      }

      if (!report.getApplicationType().equals("skein")) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("ApplicationId '" + appIdString
                             + "' is not a skein application")
            .asRuntimeException());
        return null;
      }

      return report;
    }

    @Override
    public void getStatus(Msg.Application req,
        StreamObserver<Msg.ApplicationReport> resp) {

      if (notLoggedIn(resp)) {
        return;
      }

      ApplicationReport report = getReport(req.getId(), resp);
      if (report != null) {
        resp.onNext(MsgUtils.writeApplicationReport(report));
        resp.onCompleted();
      }
    }

    @Override
    public void getLogs(Msg.LogsRequest req,
        StreamObserver<Msg.LogsResponse> resp) {

      if (notLoggedIn(resp)) {
        return;
      }

      ApplicationReport report = getReport(req.getId(), resp);
      if (report == null) {
        return;
      }

      Map<String, String> logs;
      if (hasCompleted(report)) {
        try {
          logs = getApplicationLogs(
              report.getApplicationId(), report.getUser(), req.getUser());
        } catch (LogClient.LogClientException exc) {
          resp.onError(Status.INVALID_ARGUMENT
              .withDescription(exc.getMessage())
              .asRuntimeException());
          return;
        } catch (Exception exc) {
          resp.onError(Status.INTERNAL
              .withDescription("Failed to get logs for application '"
                               + req.getId()
                               + "', exception:\n"
                               + exc.getMessage())
              .asRuntimeException());
          return;
        }
      } else {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Application " + req.getId()
                             + " has not completed, logs are not available")
            .asRuntimeException());
        return;
      }
      resp.onNext(Msg.LogsResponse.newBuilder().putAllLogs(logs).build());
      resp.onCompleted();
    }

    @Override
    public void waitForStart(Msg.Application req,
        StreamObserver<Msg.ApplicationReport> resp) {

      if (notLoggedIn(resp)) {
        return;
      }

      ApplicationReport report = getReport(req.getId(), resp);
      if (report == null) {
        return;
      }

      if (hasStarted(report)) {
        resp.onNext(MsgUtils.writeApplicationReport(report));
        resp.onCompleted();
      } else {
        addWatcher(report.getApplicationId(), resp);
      }
    }

    @Override
    public void submit(Msg.ApplicationSpec req,
        StreamObserver<Msg.Application> resp) {

      if (notLoggedIn(resp)) {
        return;
      }

      Model.ApplicationSpec spec;
      try {
        spec = MsgUtils.readApplicationSpec(req);
      } catch (Utils.UnsupportedFeatureException exc) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription(exc.getMessage())
            .asRuntimeException());
        return;
      }

      ApplicationId appId = null;
      try {
        appId = submitApplication(spec);
      } catch (Exception exc) {
        resp.onError(Status.INTERNAL
            .withDescription("Failed to submit application, "
                             + "exception:\n"
                             + exc.getMessage())
            .asRuntimeException());
        return;
      }
      resp.onNext(Msg.Application.newBuilder().setId(appId.toString()).build());
      resp.onCompleted();
    }

    @Override
    public void kill(Msg.KillRequest req, StreamObserver<Msg.Empty> resp) {

      if (notLoggedIn(resp)) {
        return;
      }

      // Check if the id is a valid skein id
      ApplicationReport report = getReport(req.getId(), resp);
      if (report == null) {
        return;
      }

      ApplicationId appId = report.getApplicationId();
      String user = req.getUser();

      try {
        killApplication(appId, user);
      } catch (Exception exc) {
        resp.onError(Status.INTERNAL
            .withDescription("Failed to kill application '"
                             + appId
                             + "', exception:\n"
                             + exc.getMessage())
            .asRuntimeException());
        return;
      }

      resp.onNext(MsgUtils.EMPTY);
      resp.onCompleted();
    }

    private String getCleanMessage(YarnException exc) {
      String msg = exc.getMessage();

      // YarnException wraps the underlying exception (which is what we care
      // about) by setting the entire remote stacktrace as the message,
      // sometimes with the exception name as a prefix (this inconsistency is
      // annoying). To get around this we strip the prefix and grab only the
      // first line (the message).
      final String prefix = "org.apache.hadoop.yarn.exceptions.YarnException: ";
      if (msg.startsWith(prefix)) {
        msg = msg.substring(prefix.length());
      }

      return msg.split("\n", 2)[0];
    }

    @Override
    public void moveApplication(Msg.MoveRequest req, StreamObserver<Msg.Empty> resp) {
      if (notLoggedIn(resp)) {
        return;
      }

      ApplicationId appId = Utils.appIdFromString(req.getId());
      if (appId == null) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Invalid ApplicationId '" + req.getId() + "'")
            .asRuntimeException());
        return;
      }
      String queue = req.getQueue();

      try {
        defaultYarnClient.moveApplicationAcrossQueues(appId, queue);
      } catch (YarnException exc) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription(getCleanMessage(exc))
            .asRuntimeException());
        return;
      } catch (IOException exc) {
        resp.onError(Status.INTERNAL
            .withDescription("Failed to move application, exception:\n"
                             + exc.getMessage())
            .asRuntimeException());
        return;
      }

      resp.onNext(MsgUtils.EMPTY);
      resp.onCompleted();
    }
  }
}
