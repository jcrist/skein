package com.anaconda.skein;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ApplicationMaster implements AMRMClientAsync.CallbackHandler,
       NMClientAsync.CallbackHandler {

  private static final Logger LOG = LogManager.getLogger(ApplicationMaster.class);

  private Configuration conf;

  private String secret;

  private Server server;

  private Model.Job job;

  private final ConcurrentHashMap<String, String> configuration =
      new ConcurrentHashMap<String, String>();

  private final Map<String, ServiceTracker> services =
      new HashMap<String, ServiceTracker>();
  private final Map<String, List<ServiceTracker>> waitingOn =
      new HashMap<String, List<ServiceTracker>>();
  private final List<ServiceTracker> trackers =
      new ArrayList<ServiceTracker>();
  private final Map<ContainerId, TrackerID> containers =
      new HashMap<ContainerId, TrackerID>();

  private Integer privatePort;
  private Integer publicPort;
  private String hostname;
  private String address;

  private AtomicInteger numTotal = new AtomicInteger();
  private AtomicInteger numSucceeded = new AtomicInteger();
  private AtomicInteger numFailed = new AtomicInteger();
  private AtomicInteger numStopped = new AtomicInteger();

  private AMRMClientAsync rmClient;
  private NMClientAsync nmClient;
  private UserGroupInformation ugi;
  private ByteBuffer tokens;

  private int loadJob() throws Exception {
    try {
      job = Utils.MAPPER.readValue(new File(".skein.json"), Model.Job.class);
    } catch (IOException exc) {
      fatal("Issue loading job specification", exc);
    }
    job.validate(true);
    LOG.info("Job successfully loaded");

    int total = 0;
    for (Model.Service service : job.getServices().values()) {
      total += service.getInstances();
    }
    LOG.info("total instances: " + total);
    return total;
  }

  private synchronized void intializeServices() throws Exception {
    for (Map.Entry<String, Model.Service> entry : job.getServices().entrySet()) {
      String serviceName = entry.getKey();
      Model.Service service = entry.getValue();

      ServiceTracker tracker = new ServiceTracker(serviceName, service);
      trackers.add(tracker);

      LOG.info("INTIALIZING: " + tracker.name);
      if (tracker.isReady()) {
        initialize(tracker);
      } else {
        for (String key : service.getDepends()) {
          List<ServiceTracker> lk = waitingOn.get(key);
          if (lk == null) {
            lk = new ArrayList<ServiceTracker>();
            waitingOn.put(key, lk);
          }
          lk.add(tracker);
        }
      }
    }
    Collections.sort(trackers);
  }

  private void startupRestServer() throws Exception {
    // Configure the server
    server = new Server();
    HandlerCollection handlers = new HandlerCollection();
    server.setHandler(handlers);
    server.setStopAtShutdown(true);

    // Create the servlets once
    final ServletHolder keystore = new ServletHolder(new KeyValueServlet());
    final ServletHolder services = new ServletHolder(new ServicesServlet());

    // This connector serves content authenticated by the secret key
    ServerConnector privateConnector = new ServerConnector(server);
    privateConnector.setPort(0);
    privateConnector.setName("Private");
    server.addConnector(privateConnector);

    ServletContextHandler privateContext =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    privateContext.setContextPath("/");
    privateContext.setVirtualHosts(new String[] {"@Private"});
    privateContext.addServlet(keystore, "/keystore/*");
    privateContext.addServlet(services, "/services/*");
    FilterHolder holder =
        privateContext.addFilter(HmacFilter.class, "/*",
                                 EnumSet.of(DispatcherType.REQUEST));
    holder.setInitParameter("secret", secret);
    handlers.addHandler(privateContext);

    // This connector serves content unauthenticated
    ServerConnector publicConnector = new ServerConnector(server);
    publicConnector.setPort(0);
    publicConnector.setName("Public");
    server.addConnector(publicConnector);

    ServletContextHandler publicContext =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    publicContext.setContextPath("/");
    publicContext.setVirtualHosts(new String[] {"@Public"});
    publicContext.addServlet(keystore, "/keystore/*");
    publicContext.addServlet(services, "/services/*");
    handlers.addHandler(publicContext);

    // Startup the server
    server.start();

    // Determine ports
    privatePort = privateConnector.getLocalPort();
    publicPort = publicConnector.getLocalPort();
    address = "http://" + hostname + ":" + privatePort;
  }

  private String formatConfig(Set<String> depends, String val) {
    if (depends != null) {
      for (String key : depends) {
        val = val.replace("%(" + key + ")", configuration.get(key));
      }
    }
    return val;
  }

  public void initialize(ServiceTracker tracker) throws IOException {
    Model.Service service = tracker.service;
    Set<String> depends = service.getDepends();

    // Finalize environment variables
    Map<String, String> env = new HashMap<String, String>();
    Map<String, String> specEnv = service.getEnv();
    if (specEnv != null) {
      for (Map.Entry<String, String> entry : specEnv.entrySet()) {
        env.put(entry.getKey(), formatConfig(depends, entry.getValue()));
      }
    }
    env.put("SKEIN_SECRET_ACCESS_KEY", secret);
    env.put("SKEIN_APPMASTER_ADDRESS", address);

    // Finalize execution script
    final StringBuilder script = new StringBuilder();
    script.append("set -e -x");
    for (String c : service.getCommands()) {
      script.append("\n");
      script.append(formatConfig(depends, c));
    }

    // Write the job script to file
    final Path scriptPath = new Path(job.getAppDir(), tracker.name + ".sh");
    LOG.info("SERVICE: " + tracker.name + " - writing script to " + scriptPath);

    LocalResource scriptResource = null;
    try {
      scriptResource = ugi.doAs(
        new PrivilegedExceptionAction<LocalResource>() {
          public LocalResource run() throws IOException {
            FileSystem fs = FileSystem.get(conf);
            OutputStream out = fs.create(scriptPath);
            try {
              out.write(script.toString().getBytes(StandardCharsets.UTF_8));
            } finally {
              out.close();
            }
            return Utils.localResource(fs, scriptPath, LocalResourceType.FILE);
          }
        });
    } catch (InterruptedException exc) { }

    // Add script to localized files
    Map<String, LocalResource> localResources;
    Map<String, LocalResource> specLR = service.getLocalResources();
    if (specLR != null) {
      localResources = new HashMap<String, LocalResource>(specLR);
    } else {
      localResources = new HashMap<String, LocalResource>();
    }
    localResources.put(".script.sh", scriptResource);

    // Build command to execute script
    ArrayList<String> commands = new ArrayList<String>();
    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    commands.add("bash .script.sh >" + logdir + "/" + tracker.name + ".log 2>&1");

    tracker.ctx = ContainerLaunchContext.newInstance(
        localResources, env, commands, null, tokens, null);

    // Request initial containers
    for (int i = 0; i < service.getInstances(); i++) {
      addContainer(tracker);
    }
  }

  @SuppressWarnings("unchecked")
  public String addContainer(ServiceTracker tracker) {
    String id = tracker.nextId();
    if (!tracker.isReady()) {
      LOG.info("WAITING: " + tracker.name + " - " + id);
      tracker.waiting.add(id);
    } else {
      ContainerRequest req =
          new ContainerRequest(tracker.service.getResources(),
                               null, null, Priority.newInstance(0));
      tracker.requested.add(id);
      rmClient.addContainerRequest(req);
      LOG.info("REQUESTED: " + tracker.name + " - " + id);
    }
    return id;
  }

  /* ResourceManager Callbacks */

  @Override
  public synchronized void onContainersCompleted(List<ContainerStatus> containerStatuses) {
    for (ContainerStatus status : containerStatuses) {

      ContainerId cid = status.getContainerId();
      int exitStatus = status.getExitStatus();

      TrackerID pair = containers.get(cid);
      if (pair == null) {
        return;  // release container that was never started
      }
      Container c = pair.tracker.running.remove(pair.id);

      if (exitStatus == ContainerExitStatus.SUCCESS) {
        LOG.info("SUCCEEDED: " + pair.tracker.name + " - " + pair.id);
        pair.tracker.succeeded.put(pair.id, c);
        numSucceeded.incrementAndGet();
      } else if (exitStatus == ContainerExitStatus.KILLED_BY_APPMASTER) {
        LOG.info("STOPPED: " + pair.tracker.name + " - " + pair.id);
        pair.tracker.stopped.put(pair.id, c);
        numStopped.incrementAndGet();
      } else {
        LOG.info("FAILED: " + pair.tracker.name + " - " + pair.id);
        pair.tracker.failed.put(pair.id, c);
        numFailed.incrementAndGet();
      }
    }

    if ((numSucceeded.get() + numStopped.get()) == numTotal.get()
        || numFailed.get() > 0) {
      shutdown();
    }
  }

  @Override
  public synchronized void onContainersAllocated(List<Container> newContainers) {
    for (Container c : newContainers) {
      boolean found = false;
      for (ServiceTracker t : trackers) {
        if (t.matches(c.getResource())) {
          found = true;
          String id = Utils.popfirst(t.requested);
          t.running.put(id, c);
          containers.put(c.getId(), new TrackerID(t, id));
          nmClient.startContainerAsync(c, t.ctx);
          LOG.info("RUNNING: " + t.name + " - " + id + " on " + c.getId());
          break;
        }
      }
      if (!found) {
        // TODO: For some reason YARN allocates extra containers *sometimes*.
        // It's not clear yet if this is a bug in skein or in YARN, for now
        // release extra containers and log the discrepancy.
        LOG.warn("No matching service round for resource: " + c.getResource()
                 + " releasing " + c.getId());
        rmClient.releaseAssignedContainer(c.getId());
      }
    }
  }

  @Override
  public void onShutdownRequest() { shutdown(); }

  @Override
  public void onError(Throwable exc) { shutdown(); }

  @Override
  public void onNodesUpdated(List<NodeReport> nodeReports) {}

  @Override
  public float getProgress() {
    return (float)(numSucceeded.get() + numStopped.get()) / numTotal.get();
  }

  /* NodeManager Callbacks */

  @Override
  public void onContainerStarted(ContainerId cid, Map<String, ByteBuffer> resp) { }

  @Override
  public void onContainerStatusReceived(ContainerId cid, ContainerStatus status) { }

  @Override
  public void onContainerStopped(ContainerId cid) { }

  @Override
  public synchronized void onStartContainerError(ContainerId containerId, Throwable exc) {
    TrackerID pair = containers.remove(containerId);
    pair.tracker.failed.put(pair.id, pair.tracker.running.remove(pair.id));
    numFailed.incrementAndGet();
  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable exc) {}

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable exc) {
    onStartContainerError(containerId, exc);
  }

  private static void fatal(String msg, Throwable exc) {
    LOG.fatal(msg, exc);
    System.exit(1);
  }

  private static void fatal(String msg) {
    LOG.fatal(msg);
    System.exit(1);
  }

  public void run() throws Exception {
    conf = new YarnConfiguration();

    secret = System.getenv("SKEIN_SECRET_ACCESS_KEY");
    if (secret == null) {
      fatal("Couldn't find secret token at 'SKEIN_SECRET_ACCESS_KEY' envar");
    }

    int totalInstances = loadJob();
    numTotal.set(totalInstances);

    try {
      hostname = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException exc) {
      fatal("Couldn't determine hostname for appmaster", exc);
    }

    // Create ugi and add original tokens to it
    String userName = System.getenv(Environment.USER.name());
    LOG.info("user: " + userName);
    ugi = UserGroupInformation.createRemoteUser(userName);

    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Remove the AM->RM token
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    ugi.addCredentials(credentials);

    startupRestServer();

    rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
    rmClient.init(conf);
    rmClient.start();

    nmClient = NMClientAsync.createNMClientAsync(this);
    nmClient.init(conf);
    nmClient.start();

    rmClient.registerApplicationMaster(hostname, privatePort, "");

    intializeServices();

    server.join();
  }

  private void shutdown() {
    // wait for completion.
    nmClient.stop();

    FinalApplicationStatus status;
    if (numFailed.get() == 0
        && (numSucceeded.get() + numStopped.get()) == numTotal.get()) {
      status = FinalApplicationStatus.SUCCEEDED;
    } else {
      status = FinalApplicationStatus.FAILED;
    }

    String msg = ("Diagnostics."
                  + ", total = " + numTotal.get()
                  + ", succeeded = " + numSucceeded.get()
                  + ", stopped = " + numStopped.get()
                  + ", failed = " + numFailed.get());

    try {
      rmClient.unregisterApplicationMaster(status, msg, null);
    } catch (Exception ex) {
      LOG.error("Failed to unregister application", ex);
    }

    rmClient.stop();
    try {
      server.stop();
    } catch (InterruptedException ex) {
      // Raised by jetty to stop server
    } catch (Exception ex) {
      LOG.error("Failed to properly shutdown the jetty server", ex);
    }
  }

  /** Main entrypoint for the ApplicationMaster. **/
  public static void main(String[] args) {
    ApplicationMaster appMaster = new ApplicationMaster();

    try {
      appMaster.run();
    } catch (Throwable exc) {
      fatal("Error running ApplicationMaster", exc);
    }
  }

  private class ServicesServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {

      String path = Utils.getPath(req);
      Map<String, Model.Service> services = job.getServices();

      if (path == null) {
        resp.setHeader("Content-Type", "application/json");
        OutputStream out = resp.getOutputStream();
        Utils.MAPPER.writeValue(out, services);
        out.close();
        return;
      }

      Model.Service service = services.get(path);
      if (service == null) {
        Utils.sendError(resp, 404, "Unknown service");
        return;
      }

      resp.setHeader("Content-Type", "application/json");
      OutputStream out = resp.getOutputStream();
      Utils.MAPPER.writeValue(out, service);
      out.close();
    }
  }

  private class KeyValueServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {

      String key = Utils.getPath(req);

      if (key == null) {
        // Handle /keystore or /keystore/
        resp.setHeader("Content-Type", "application/json");
        OutputStream out = resp.getOutputStream();
        Utils.MAPPER.writeValue(out, configuration);
        out.close();
        return;
      }

      String value = configuration.get(key);
      if (value == null) {
        Utils.sendError(resp, 404, "Missing key");
        return;
      }

      OutputStream out = resp.getOutputStream();
      out.write(value.getBytes(StandardCharsets.UTF_8));
      out.close();
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {

      String key = Utils.getPath(req);
      byte[] bytes = IOUtils.toByteArray(req.getInputStream());

      if (key == null || bytes.length == 0) {
        Utils.sendError(resp, 400, "Malformed Request");
        return;
      }

      String value = new String(bytes, StandardCharsets.UTF_8);
      String current = configuration.get(key);

      // If key exists and doesn't match value (allows for idempotent requests)
      if (current != null && !value.equals(current)) {
        Utils.sendError(resp, 403, "Key already set");
        return;
      }

      configuration.put(key, value);

      // Notify dependent services
      synchronized (ApplicationMaster.this) {
        if (waitingOn.containsKey(key)) {
          for (ServiceTracker s: waitingOn.remove(key)) {
            if (s.notifySet()) {
              initialize(s);
            }
          }
        }
      }

      resp.setStatus(204);
    }
  }

  private static class ServiceTracker implements Comparable<ServiceTracker> {
    public String name;
    public Model.Service service;
    public ContainerLaunchContext ctx;
    public final Set<String> waiting = new LinkedHashSet<String>();
    public final Set<String> requested = new LinkedHashSet<String>();
    public final Map<String, Container> running = new HashMap<String, Container>();
    public final Map<String, Container> succeeded = new HashMap<String, Container>();
    public final Map<String, Container> failed = new HashMap<String, Container>();
    public final Map<String, Container> stopped = new HashMap<String, Container>();

    private AtomicInteger numWaitingOn;
    private int count = 0;

    public ServiceTracker(String name, Model.Service service) {
      this.name = name;
      this.service = service;
      Set<String> depends = service.getDepends();
      int size = (depends == null) ? 0 : depends.size();
      numWaitingOn = new AtomicInteger(size);
    }

    public String nextId() {
      count += 1;
      return name + "_" + count;
    }

    public boolean isReady() { return numWaitingOn.get() == 0; }

    public boolean matches(Resource r) {
      // requested and requirement <= response
      return requested.size() > 0 && service.getResources().compareTo(r) <= 0;
    }

    public boolean notifySet() {
      return numWaitingOn.decrementAndGet() == 0;
    }

    public int compareTo(ServiceTracker other) {
      return service.getResources().compareTo(other.service.getResources());
    }
  }

  public static class TrackerID {
    public ServiceTracker tracker;
    public String id;

    public TrackerID(ServiceTracker tracker, String id) {
      this.tracker = tracker;
      this.id = id;
    }
  }
}
