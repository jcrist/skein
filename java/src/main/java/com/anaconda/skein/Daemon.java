package com.anaconda.skein;

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
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class Daemon {

  private static final Logger LOG = LogManager.getLogger(Daemon.class);

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

  private FileSystem defaultFs;

  private YarnClient yarnClient;

  private String classpath;

  private String jarPath;
  private String certPath;
  private String keyPath;

  private boolean daemon = false;

  private int amMemory = 512;
  private int amVCores = 1;

  private int callbackPort;

  private String secret;

  private Server server;

  private final Map<ApplicationId, List<StreamObserver<Msg.ApplicationReport>>> startedCallbacks =
      new HashMap<ApplicationId, List<StreamObserver<Msg.ApplicationReport>>>();

  private void startServer() throws IOException {
    // Setup and start the server
    SslContext sslContext = GrpcSslContexts
        .forServer(new File(certPath), new File(keyPath))
        .trustManager(new File(certPath))
        .clientAuth(ClientAuth.REQUIRE)
        .sslProvider(SslProvider.OPENSSL)
        .build();

    NioEventLoopGroup eg = new NioEventLoopGroup(NUM_EVENT_LOOP_GROUP_THREADS);
    ThreadPoolExecutor executor = Utils.newThreadPoolExecutor(
        "grpc-executor",
        MIN_GRPC_EXECUTOR_THREADS,
        MAX_GRPC_EXECUTOR_THREADS,
        true);

    server = NettyServerBuilder.forPort(0)
        .sslContext(sslContext)
        .addService(new DaemonImpl())
        .workerEventLoopGroup(eg)
        .bossEventLoopGroup(eg)
        .executor(executor)
        .build()
        .start();

    LOG.info("Server started, listening on " + server.getPort());

    Runtime.getRuntime().addShutdownHook(
        new Thread() {
          @Override
          public void run() {
            Daemon.this.stopServer();
          }
        });
  }

  private void stopServer() {
    if (server != null) {
      LOG.info("Shutting down gRPC server");
      server.shutdown();
      LOG.info("gRPC server shut down");
    }
  }

  /** Main Entry Point. **/
  public static void main(String[] args) {
    boolean result = false;
    try {
      Daemon daemon = new Daemon();
      daemon.init(args);
      daemon.run();
      System.exit(0);
    } catch (Throwable exc) {
      LOG.fatal("Error running Daemon", exc);
      System.exit(1);
    }
  }

  private void init(String[] args) throws IOException {
    String callbackPortEnv = System.getenv("SKEIN_CALLBACK_PORT");
    if (callbackPortEnv == null) {
      LOG.fatal("Couldn't find 'SKEIN_CALLBACK_PORT' envar");
      System.exit(1);
    }
    callbackPort = Integer.valueOf(callbackPortEnv);

    // Parse arguments
    if (args.length < 3 || args.length > 4
        || (args.length == 4 && !args[3].equals("--daemon"))) {
      LOG.fatal("Usage: COMMAND jarPath certPath keyPath [--daemon]");
      System.exit(1);
    }
    jarPath = args[0];
    certPath = args[1];
    keyPath = args[2];
    daemon = args.length == 4;

    conf = new YarnConfiguration();
    defaultFs = FileSystem.get(conf);

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
    // Start the yarn client
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    // Start the server
    startServer();

    // Report back the port we're listening on
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
      LOG.info("Starting process disconnected, shutting down");
    }
  }

  /** Kill a running application. **/
  public boolean killApplication(ApplicationId appId) {
    try {
      yarnClient.killApplication(appId);
    } catch (Throwable exc) {
      return false;
    }
    return true;
  }

  /** Start a new application. **/
  public ApplicationId submitApplication(Model.ApplicationSpec spec)
      throws IOException, YarnException {
    // First validate the spec request
    spec.validate();

    // Get an application id. This is needed before doing anything else so we
    // can upload additional files to the application directory
    YarnClientApplication app = yarnClient.createApplication();
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    // Setup the LocalResources for the appmaster and containers
    Path appDir = new Path(defaultFs.getHomeDirectory(),
                           ".skein/" + appId.toString());
    Map<String, LocalResource> localResources = setupAppDir(spec, appDir);

    // Setup the appmaster environment variables
    Map<String, String> env = new HashMap<String, String>();
    env.put("CLASSPATH", classpath);

    // Setup the appmaster commands
    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    List<String> commands = Arrays.asList(
        (Environment.JAVA_HOME.$$() + "/bin/java "
         + "-Xmx128M "
         + "com.anaconda.skein.ApplicationMaster "
         + appDir
         + " >" + logdir + "/appmaster.log 2>&1"));

    // Add security tokens as needed
    ByteBuffer fsTokens = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException("Can't determine Yarn ResourceManager Kerberos "
                              + "principal for the RM to use as renewer");
      }

      defaultFs.addDelegationTokens(tokenRenewer, credentials);
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    ContainerLaunchContext amContext = ContainerLaunchContext.newInstance(
        localResources, env, commands, null, fsTokens, null);

    appContext.setApplicationType("skein");
    appContext.setAMContainerSpec(amContext);
    appContext.setApplicationName(spec.getName());
    appContext.setResource(Resource.newInstance(amMemory, amVCores));
    appContext.setPriority(Priority.newInstance(0));
    appContext.setQueue(spec.getQueue());
    appContext.setMaxAppAttempts(spec.getMaxAttempts());
    appContext.setApplicationTags(spec.getTags());

    LOG.info("Submitting application...");
    yarnClient.submitApplication(appContext);

    return appId;
  }

  private Map<String, LocalResource> setupAppDir(Model.ApplicationSpec spec,
        Path appDir) throws IOException {

    // Make the ~/.skein/app_id dir
    FileSystem.mkdirs(defaultFs, appDir, SKEIN_DIR_PERM);

    Map<Path, Path> uploadCache = new HashMap<Path, Path>();

    // Create LocalResources for the crt/pem files
    LocalResource certFile = newLocalResource(uploadCache, appDir, certPath);
    LocalResource keyFile = newLocalResource(uploadCache, appDir, keyPath);

    // Setup the LocalResources for the services
    for (Map.Entry<String, Model.Service> entry: spec.getServices().entrySet()) {
      finalizeService(entry.getKey(), entry.getValue(),
                      uploadCache, appDir, certFile, keyFile);
    }
    spec.validate();

    // Write the application specification to file
    Path specPath = new Path(appDir, ".skein.proto");
    LOG.info("Writing application specification to " + specPath);
    OutputStream out = defaultFs.create(specPath);
    try {
      MsgUtils.writeApplicationSpec(spec).writeTo(out);
    } finally {
      out.close();
    }
    LocalResource specFile = Utils.localResource(defaultFs, specPath,
                                                 LocalResourceType.FILE);

    // Setup the LocalResources for the application master
    Map<String, LocalResource> lr = new HashMap<String, LocalResource>();
    lr.put("skein.jar", newLocalResource(uploadCache, appDir, jarPath));
    lr.put(".skein.crt", certFile);
    lr.put(".skein.pem", keyFile);
    lr.put(".skein.proto", specFile);
    return lr;
  }

  private void finalizeService(String serviceName, Model.Service service,
      Map<Path, Path> uploadCache, Path appDir,
      LocalResource certFile, LocalResource keyFile) throws IOException {

    // Build the execution script
    final StringBuilder script = new StringBuilder();
    script.append("set -e -x");
    for (String c : service.getCommands()) {
      script.append("\n");
      script.append(c);
    }

    // Write the service script to file
    final Path scriptPath = new Path(appDir, serviceName + ".sh");
    LOG.info("SERVICE: " + serviceName + " - writing script to " + scriptPath);

    OutputStream out = defaultFs.create(scriptPath);
    try {
      out.write(script.toString().getBytes(StandardCharsets.UTF_8));
    } finally {
      out.close();
    }
    LocalResource scriptFile = Utils.localResource(defaultFs, scriptPath,
                                                   LocalResourceType.FILE);

    // Build command to execute script and set as new commands
    ArrayList<String> commands = new ArrayList<String>();
    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    commands.add("bash .script.sh >" + logdir + "/" + serviceName + ".log 2>&1");
    service.setCommands(commands);

    // Upload files/archives as necessary
    Map<String, LocalResource> lr = service.getLocalResources();
    for (LocalResource resource : lr.values()) {
      finalizeLocalResource(uploadCache, appDir, resource, true);
    }

    // Add script/crt/pem files
    lr.put(".script.sh", scriptFile);
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
        LOG.info("Uploading " + srcPath + " to " + dstPath);
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

  private synchronized void addWatcher(final ApplicationId appId,
      StreamObserver<Msg.ApplicationReport> resp) {

    if (startedCallbacks.get(appId) != null) {
      startedCallbacks.get(appId).add(resp);
      return;
    }

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
              report = yarnClient.getApplicationReport(appId);
            } catch (Exception exc) {
              LOG.warn("Failed to get report for " + appId.toString()
                       + ". Notifying " + callbacks.size() + " callbacks.");
              for (StreamObserver<Msg.ApplicationReport> resp : callbacks) {
                // Send error
                try {
                  resp.onError(Status.INTERNAL
                      .withDescription("Failed to get applications, exception:\n"
                                      + exc.getMessage())
                      .asRuntimeException());
                } catch (StatusRuntimeException cbExc) {
                  if (cbExc.getStatus().getCode() != Status.Code.CANCELLED) {
                    LOG.warn("Callback failed for app_id: " + appId.toString()
                             + ", status: " + cbExc.getStatus());
                  }
                }
              }
              break;
            }

            if (hasStarted(report)) {
              LOG.info("Notifying that " + appId.toString() + " has started. "
                       + callbacks.size() + " callbacks registered.");
              for (StreamObserver<Msg.ApplicationReport> resp : callbacks) {
                // Send report
                try {
                  resp.onNext(MsgUtils.writeApplicationReport(report));
                  resp.onCompleted();
                } catch (StatusRuntimeException cbExc) {
                  if (cbExc.getStatus().getCode() != Status.Code.CANCELLED) {
                    LOG.warn("Callback failed for app_id: " + appId.toString()
                             + ", status: " + cbExc.getStatus());
                  }
                }
              }
              break;
            }

            // Sleep for 1 second
            try {
              thisThread.sleep(1000);
            } catch (InterruptedException exc) { }
          }

          // Remove callbacks for this appId
          LOG.info("Removing callbacks for " + appId.toString());
          synchronized (Daemon.this) {
            startedCallbacks.remove(appId);
          }
        }
      };
    watcher.setDaemon(true);
    watcher.start();
  }

  class DaemonImpl extends DaemonGrpc.DaemonImplBase {
    @Override
    public void ping(Msg.Empty req, StreamObserver<Msg.Empty> resp) {
      resp.onNext(MsgUtils.EMPTY);
      resp.onCompleted();
    }

    @Override
    public void getApplications(Msg.ApplicationsRequest req,
        StreamObserver<Msg.ApplicationsResponse> resp) {

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
        reports = yarnClient.getApplications(
          new HashSet<String>(Arrays.asList("skein")), states);
      } catch (Exception exc) {
        resp.onError(Status.INTERNAL
            .withDescription("Failed to get applications, exception:\n"
                            + exc.getMessage())
            .asRuntimeException());
        return;
      }

      resp.onNext(MsgUtils.writeApplicationsResponse(reports));
      resp.onCompleted();
    }

    private ApplicationReport getReport(Msg.Application req,
        StreamObserver<?> resp) {

      final ApplicationId appId = Utils.appIdFromString(req.getId());

      if (appId == null) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Invalid ApplicationId '" + req.getId() + "'")
            .asRuntimeException());
        return null;
      }

      ApplicationReport report;
      try {
        report = yarnClient.getApplicationReport(appId);
      } catch (Exception exc) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Unknown ApplicationId '" + req.getId() + "'")
            .asRuntimeException());
        return null;
      }

      if (!report.getApplicationType().equals("skein")) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("ApplicationId '" + req.getId()
                             + "' is not a skein application")
            .asRuntimeException());
        return null;
      }

      return report;
    }

    @Override
    public void getStatus(Msg.Application req,
        StreamObserver<Msg.ApplicationReport> resp) {

      ApplicationReport report = getReport(req, resp);
      if (report != null) {
        resp.onNext(MsgUtils.writeApplicationReport(report));
        resp.onCompleted();
      }
    }

    @Override
    public void waitForStart(Msg.Application req,
        StreamObserver<Msg.ApplicationReport> resp) {

      ApplicationReport report = getReport(req, resp);
      if (report == null) {
        return;  // error message set in getReport
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

      Model.ApplicationSpec spec = MsgUtils.readApplicationSpec(req);

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
    public void kill(Msg.Application req, StreamObserver<Msg.Empty> resp) {
      // Check if the id is a valid skein id
      ApplicationReport report = getReport(req, resp);
      if (report == null) {
        return;
      }

      try {
        yarnClient.killApplication(report.getApplicationId());
      } catch (Exception exc) {
        resp.onError(Status.INTERNAL
            .withDescription("Failed to kill application '"
                             + report.getApplicationId()
                             + "' , exception:\n"
                             + exc.getMessage())
            .asRuntimeException());
        return;
      }

      resp.onNext(MsgUtils.EMPTY);
      resp.onCompleted();
    }
  }
}
