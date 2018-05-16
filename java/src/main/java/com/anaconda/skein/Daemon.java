package com.anaconda.skein;

import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class Daemon {

  private static final Logger LOG = LogManager.getLogger(Daemon.class);

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

  private void startServer() throws IOException {
    // Setup and start the server
    SslContext sslContext = GrpcSslContexts
        .forServer(new File(certPath), new File(keyPath))
        .trustManager(new File(certPath))
        .clientAuth(ClientAuth.REQUIRE)
        .sslProvider(SslProvider.OPENSSL)
        .build();

    server = NettyServerBuilder.forPort(0)
        .sslContext(sslContext)
        .addService(new DaemonImpl())
        .build()
        .start();

    LOG.info("Server started, listening on " + server.getPort());

    Runtime.getRuntime().addShutdownHook(
        new Thread() {
          @Override
          public void run() {
            System.err.println("*** shutting down gRPC server");
            Daemon.this.stopServer();
            System.err.println("*** gRPC server shut down");
          }
        });
  }

  private void stopServer() {
    if (server != null) {
      server.shutdown();
    }
  }

  /** Main Entry Point. **/
  public static void main(String[] args) {
    boolean result = false;
    try {
      Daemon daemon = new Daemon();
      daemon.init(args);
      daemon.run();
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
      stopServer();
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
  public ApplicationId submitApplication(Model.Job job)
      throws IOException, YarnException {
    // First validate the job request
    job.validate();

    // Get an application id. This is needed before doing anything else so we
    // can upload additional files to the application directory
    YarnClientApplication app = yarnClient.createApplication();
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    // Setup the LocalResources for the appmaster and containers
    Path appDir = new Path(defaultFs.getHomeDirectory(),
                           ".skein/" + appId.toString());
    Map<String, LocalResource> localResources = setupAppDir(job, appDir);

    // Setup the appmaster environment variables
    Map<String, String> env = new HashMap<String, String>();
    env.put("CLASSPATH", classpath);

    // Setup the appmaster commands
    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    List<String> commands = Arrays.asList(
        (Environment.JAVA_HOME.$$() + "/bin/java "
         + "-Xmx10M "
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
    appContext.setApplicationName(job.getName());
    appContext.setResource(Resource.newInstance(amMemory, amVCores));
    appContext.setPriority(Priority.newInstance(0));
    appContext.setQueue(job.getQueue());

    LOG.info("Submitting application...");
    yarnClient.submitApplication(appContext);

    return appId;
  }

  private Map<String, LocalResource> setupAppDir(Model.Job job,
        Path appDir) throws IOException {

    // Make the ~/.skein/app_id dir
    FileSystem.mkdirs(defaultFs, appDir, SKEIN_DIR_PERM);

    Map<Path, Path> uploadCache = new HashMap<Path, Path>();

    // Create LocalResources for the crt/pem files
    LocalResource certFile = newLocalResource(uploadCache, appDir, certPath);
    LocalResource keyFile = newLocalResource(uploadCache, appDir, keyPath);

    // Setup the LocalResources for the services
    for (Model.Service s: job.getServices().values()) {
      finalizeService(s, uploadCache, appDir, certFile, keyFile);
    }
    job.validate();

    // Write the job specification to file
    Path specPath = new Path(appDir, ".skein.proto");
    LOG.info("Writing job specification to " + specPath);
    OutputStream out = defaultFs.create(specPath);
    try {
      MsgUtils.writeJob(job).writeTo(out);
    } finally {
      out.close();
    }
    FileStatus status = defaultFs.getFileStatus(specPath);
    LocalResource spec = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(specPath),
        LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION,
        status.getLen(),
        status.getModificationTime());

    // Setup the LocalResources for the application master
    Map<String, LocalResource> lr = new HashMap<String, LocalResource>();
    lr.put("skein.jar", newLocalResource(uploadCache, appDir, jarPath));
    lr.put(".skein.crt", certFile);
    lr.put(".skein.pem", keyFile);
    lr.put(".skein.proto", spec);
    return lr;
  }

  private void finalizeService(Model.Service service,
      Map<Path, Path> uploadCache, Path appDir,
      LocalResource certFile, LocalResource keyFile) throws IOException {
    // Upload files/archives as necessary
    Map<String, LocalResource> lr = service.getLocalResources();
    for (LocalResource resource : lr.values()) {
      finalizeLocalResource(uploadCache, appDir, resource, true);
    }
    // Add crt/pem files
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
    LocalResource out = LocalResource.newInstance(Utils.urlFromString(localPath),
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

        file.setResource(ConverterUtils.getYarnUrlFromPath(dstPath));
      }
      uploadCache.put(srcPath, dstPath);
    }
    FileStatus status = dstFs.getFileStatus(dstPath);

    // Only set size & timestamp if not set already
    if (file.getSize() == 0) {
      file.setSize(status.getLen());
    }

    if (file.getTimestamp() == 0) {
      file.setTimestamp(status.getModificationTime());
    }
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

      ApplicationId appId = Utils.appIdFromString(req.getId());

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
    public void submit(Msg.Job req,
        StreamObserver<Msg.Application> resp) {

      Model.Job job = MsgUtils.readJob(req);

      ApplicationId appId = null;
      try {
        appId = submitApplication(job);
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
