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
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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

  private boolean daemon = false;

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
    context.addServlet(new ServletHolder(new AppsServlet()), "/apps/*");
    context.addServlet(new ServletHolder(new SkeinServlet()), "/skein/*");
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

    if (args.length > 2) {
      LOG.fatal("Unknown extra arguments");
      System.exit(1);
    }
    for (String arg : args) {
      if (arg.equals("--daemon")) {
        daemon = true;
      } else {
        amJar = arg;
      }
    }
    if (amJar == null) {
      LOG.fatal("Must pass in path to this jar file");
      System.exit(1);
    }

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

    if (daemon) {
      server.join();
    } else {
      // Wait until EOF or broken pipe from stdin
      while (System.in.read() != -1) {}
      server.stop();
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
        ApplicationId appId) throws IOException {

    // Make the ~/.skein/app_id dir
    Path appDir = new Path(defaultFs.getHomeDirectory(), ".skein/" + appId.toString());
    FileSystem.mkdirs(defaultFs, appDir, SKEIN_DIR_PERM);

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
    // Upload files/archives as necessary
    if (service.getFiles() != null) {
      Map<String, LocalResource> lr = new HashMap<String, LocalResource>();
      for (Model.File f : service.getFiles()) {
        addFile(uploadCache, appDir, lr, f.getSource(), f.getDest(), f.getType());
      }
      service.setLocalResources(lr);
    }
    // Add LANG if present
    String lang = System.getenv("LANG");
    if (lang != null) {
      if (service.getEnv() == null) {
        service.setEnv(new HashMap<String, String>());
      }
      service.getEnv().put("LANG", lang);
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
        dstPath = new Path(new Path(appDir, prefix), srcPath.getName());
        LOG.info("Uploading " + srcPath + " to " + dstPath);
        FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, conf);
        dstFs.setPermission(dstPath, SKEIN_FILE_PERM);
        uploadCache.put(srcPath, dstPath);
      }
    }

    localResources.put(dst, Utils.localResource(defaultFs, dstPath, type));
  }

  private final class SkeinServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      if (daemon && "shutdown".equals(Utils.getPath(req))) {
        new Thread() {
          @Override
          public void run() {
            try {
              LOG.info("Shutdown signal received, shutting down");
              server.stop();
            } catch (Exception ex) {
              LOG.info("Failed to stop Jetty", ex);
              System.exit(0);
            }
          }
        }.start();
      } else {
        Utils.sendError(resp, 404);
      }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      if (req.getPathInfo() != null) {
        Utils.sendError(resp, 404);
        return;
      }
    }
  }

  private final class AppsServlet extends HttpServlet {
    private final Set<String> skeinSet =
        new HashSet<String>(Arrays.asList("skein"));

    private ApplicationId appIdFromString(String appId) {
      // Parse applicationId_{timestamp}_{id}
      String[] parts = appId.split("_");
      if (parts.length < 3) {
        return null;
      }
      long timestamp = Long.valueOf(parts[1]);
      int id = Integer.valueOf(parts[2]);
      return ApplicationId.newInstance(timestamp, id);
    }

    private ApplicationId getAppId(HttpServletRequest req) {
      String appId = req.getPathInfo();
      if (appId == null || appId.length() <= 1) {
        return null;
      }
      return appIdFromString(appId.substring(1));
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {

      ApplicationId appId = getAppId(req);

      if (appId == null) {
        String[] statesReq = req.getParameterValues("state");
        EnumSet<YarnApplicationState> states;
        if (statesReq == null) {
          states = EnumSet.of(YarnApplicationState.SUBMITTED,
                              YarnApplicationState.ACCEPTED,
                              YarnApplicationState.RUNNING);
        } else {
          states = EnumSet.noneOf(YarnApplicationState.class);
          for (String s : statesReq) {
            try {
              states.add(YarnApplicationState.valueOf(s));
            } catch (IllegalArgumentException exc) { }
          }
        }

        List<ApplicationReport> reports;
        try {
          reports = yarnClient.getApplications(skeinSet, states);
        } catch (YarnException exc) {
          Utils.sendError(resp, 500, exc.toString());
          return;
        }

        OutputStream out = resp.getOutputStream();
        Utils.MAPPER.writeValue(out, reports);
        out.close();
      } else {
        ApplicationReport report;
        try {
          report = yarnClient.getApplicationReport(appId);
        } catch (YarnException exc) {
          Utils.sendError(resp, 404, "Unknown ApplicationID");
          return;
        }
        if (!report.getApplicationType().equals("skein")) {
          Utils.sendError(resp, 404, "Not a skein application");
        }
        OutputStream out = resp.getOutputStream();
        Utils.MAPPER.writeValue(out, report);
        out.close();
      }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {

      ApplicationId appId = getAppId(req);

      if (appId != null) {
        Utils.sendError(resp, 400, "Malformed Request");
        return;
      }

      Model.Job job;
      try {
        job = Utils.MAPPER.readValue(req.getInputStream(), Model.Job.class);
      } catch (Exception exc) {
        Utils.sendError(resp, 400, exc.getMessage());
        return;
      }

      try {
        appId = submit(job);
      } catch (Exception exc) {
        Utils.sendError(resp, 400, exc.getMessage());
        return;
      }
      OutputStream out = resp.getOutputStream();
      out.write(appId.toString().getBytes());
      resp.setStatus(200);
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {

      ApplicationId appId = getAppId(req);

      if (appId == null) {
        Utils.sendError(resp, 400, "Malformed Request");
        return;
      }

      if (killApplication(appId)) {
        resp.setStatus(204);
      } else {
        Utils.sendError(resp, 404, "Failed to kill application");
      }
    }
  }
}
