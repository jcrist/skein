package com.anaconda.skein;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.DispatcherType;

public class Client {

  private static final Logger LOG = LogManager.getLogger(Client.class);

  // Owner rwx (700)
  private static final FsPermission SKEIN_DIR_PERM =
      FsPermission.createImmutable((short)448);
  // Owner rw, world r (644)
  private static final FsPermission SKEIN_FILE_PERM =
      FsPermission.createImmutable((short)420);

  private Configuration conf;

  private FileSystem defaultFs;

  private YarnClient yarnClient;

  private String classpath;

  private String amJar;

  private int amMemory = 10;

  private int amVCores = 1;

  private int callbackPort;

  private int port;

  private String secret;

  private Server server;

  private void startupRestServer() throws Exception {
    // Configure the server
    server = new Server();
    HandlerCollection handlers = new HandlerCollection();
    server.setHandler(handlers);
    server.setStopAtShutdown(true);

    ServerConnector connector = new ServerConnector(server);
    connector.setHost("127.0.0.1");
    connector.setPort(0);
    server.addConnector(connector);

    ServletContextHandler context =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    context.addServlet(new ServletHolder(new ClientServlet(this)), "/apps/*");
    FilterHolder holder = context.addFilter(HmacFilter.class, "/*",
                                            EnumSet.of(DispatcherType.REQUEST));
    holder.setInitParameter("secret", secret);
    handlers.addHandler(context);

    // Startup the server
    server.start();

    // Determine port
    port = connector.getLocalPort();
  }

  /** Main Entry Point. **/
  public static void main(String[] args) {
    boolean result = false;
    try {
      Client client = new Client();
      client.init(args);
      client.run();
    } catch (Throwable exc) {
      LOG.fatal("Error running Client", exc);
      System.exit(1);
    }
  }

  private void init(String[] args) throws IOException {
    secret = System.getenv("SKEIN_SECRET_ACCESS_KEY");
    if (secret == null) {
      LOG.fatal("Couldn't find 'SKEIN_SECRET_ACCESS_KEY' envar");
      System.exit(1);
    }

    String callbackPortEnv = System.getenv("SKEIN_CALLBACK_PORT");
    if (callbackPortEnv == null) {
      LOG.fatal("Couldn't find 'SKEIN_CALLBACK_PORT' envar");
      System.exit(1);
    }
    callbackPort = Integer.valueOf(callbackPortEnv);

    if (args.length < 1) {
      LOG.fatal("Must pass in path to this jar file");
      System.exit(1);
    } else if (args.length > 1) {
      LOG.fatal("Unknown extra arguments");
      System.exit(1);
    }
    amJar = args[0];

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

    // Start the rest server
    startupRestServer();

    // Report back the port we're listening on
    Socket callback = new Socket("127.0.0.1", callbackPort);
    DataOutputStream dos = new DataOutputStream(callback.getOutputStream());
    dos.writeInt(port);
    dos.close();
    callback.close();

    // Wait until EOF or broken pipe from stdin
    while (System.in.read() != -1) {}
    server.stop();
  }

  /** Get information on a running application. **/
  public ApplicationReport getApplicationReport(ApplicationId appId) {
    try {
      return yarnClient.getApplicationReport(appId);
    } catch (Throwable exc) {
      return null;
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
  public ApplicationId submit(Model.Job job) throws IOException, YarnException {
    // First validate the job request
    job.validate(false);

    // Get an application id. This is needed before doing anything else so we
    // can upload additional files to the application directory
    YarnClientApplication app = yarnClient.createApplication();
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    // Setup the LocalResources for the appmaster and containers
    Map<String, LocalResource> localResources = setupAppDir(job, appId);

    // Setup the appmaster environment variables
    Map<String, String> env = new HashMap<String, String>();
    env.put("CLASSPATH", classpath);
    env.put("SKEIN_SECRET_ACCESS_KEY", secret);

    // Setup the appmaster commands
    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    List<String> commands = Arrays.asList(
        (Environment.JAVA_HOME.$$() + "/bin/java "
         + "-Xmx" + amMemory + "m "
         + "com.anaconda.skein.ApplicationMaster "
         + ">" + logdir + "/appmaster.log 2>&1"));

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
        ApplicationId appId) throws IOException {

    // Make the ~/.skein/app_id and ~/.skein/app_id/services dirs
    Path appDir = new Path(defaultFs.getHomeDirectory(), ".skein/" + appId.toString());
    FileSystem.mkdirs(defaultFs, appDir, SKEIN_DIR_PERM);
    FileSystem.mkdirs(defaultFs, new Path(appDir, "services"), SKEIN_DIR_PERM);

    Map<Path, Path> uploadCache = new HashMap<Path, Path>();

    // Setup the LocalResources for the services
    for (Model.Service s: job.getServices().values()) {
      finalizeService(s, uploadCache, appDir);
    }
    job.setAppDir(appDir.toString());
    job.validate(true);

    // Write the job specification to file
    Path specPath = new Path(appDir, ".skein.json");
    LOG.info("Writing job specification to " + specPath);
    OutputStream out = defaultFs.create(specPath);
    try {
      Utils.MAPPER.writeValue(out, job);
    } finally {
      out.close();
    }

    // Setup the LocalResources for the application master
    Map<String, LocalResource> lr = new HashMap<String, LocalResource>();
    addFile(uploadCache, appDir, lr, amJar, "skein.jar", LocalResourceType.FILE);
    lr.put(".skein.json", Utils.localResource(defaultFs, specPath,
                                              LocalResourceType.FILE));

    return lr;
  }

  private void finalizeService(Model.Service service, Map<Path, Path> uploadCache,
                               Path appDir) throws IOException {
    if (service.getFiles() != null) {
      Map<String, LocalResource> lr = new HashMap<String, LocalResource>();
      for (Model.File f : service.getFiles()) {
        addFile(uploadCache, appDir, lr, f.getSource(), f.getDest(), f.getType());
      }
      service.setLocalResources(lr);
    }
    service.setFiles(null);
  }

  private void addFile(Map<Path, Path> uploadCache, Path appDir,
                       Map<String, LocalResource> localResources,
                       String source, String dst, LocalResourceType type)
      throws IOException {
    Path srcPath = Utils.normalizePath(source);
    Path dstPath = srcPath;

    FileSystem dstFs = appDir.getFileSystem(conf);
    FileSystem srcFs = srcPath.getFileSystem(conf);

    if (!Utils.equalFs(srcFs, dstFs)) {
      dstPath = uploadCache.get(srcPath);
      if (dstPath == null) {
        // File needs to be uploaded to the destination filesystem
        MessageDigest md;
        try {
          md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
          throw new IllegalArgumentException("MD5 not supported on this platform");
        }
        md.update(srcPath.toString().getBytes());
        String prefix = Utils.hexEncode(md.digest());
        dstPath = new Path(appDir, prefix + "-" + srcPath.getName());
        LOG.info("Uploading " + srcPath + " to " + dstPath);
        FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, conf);
        dstFs.setPermission(dstPath, SKEIN_FILE_PERM);
        uploadCache.put(srcPath, dstPath);
      }
    }

    localResources.put(dst, Utils.localResource(defaultFs, dstPath, type));
  }
}
