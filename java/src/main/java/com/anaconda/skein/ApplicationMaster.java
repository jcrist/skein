package com.anaconda.skein;

import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMaster implements AMRMClientAsync.CallbackHandler,
       NMClientAsync.CallbackHandler {

  private static final Logger LOG = LogManager.getLogger(ApplicationMaster.class);

  private Configuration conf;

  private Model.Job job;

  private Path appDir;

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

  private Server server;
  private String hostname;
  private int port = -1;

  private AtomicInteger numTotal = new AtomicInteger();
  private AtomicInteger numSucceeded = new AtomicInteger();
  private AtomicInteger numFailed = new AtomicInteger();
  private AtomicInteger numStopped = new AtomicInteger();

  private AMRMClientAsync rmClient;
  private NMClientAsync nmClient;
  private UserGroupInformation ugi;
  private ByteBuffer tokens;

  private void startServer() throws IOException {
    // Setup and start the server
    server = NettyServerBuilder.forPort(0)
        .addService(new MasterImpl())
        .build()
        .start();

    port = server.getPort();

    LOG.info("Server started, listening on " + port);

    Runtime.getRuntime().addShutdownHook(
        new Thread() {
          @Override
          public void run() {
            System.err.println("*** shutting down gRPC server");
            ApplicationMaster.this.stopServer();
            System.err.println("*** gRPC server shut down");
          }
        });
  }

  private void stopServer() {
    if (server != null) {
      server.shutdown();
    }
  }

  private int loadJob() throws Exception {
    try {
      job = MsgUtils.readJob(Msg.Job.parseFrom(new FileInputStream(".skein.proto")));
    } catch (IOException exc) {
      fatal("Issue loading job specification", exc);
    }
    job.validate();
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
    env.put("SKEIN_APPMASTER_ADDRESS", hostname + ":" + port);

    // Finalize execution script
    final StringBuilder script = new StringBuilder();
    script.append("set -e -x");
    for (String c : service.getCommands()) {
      script.append("\n");
      script.append(formatConfig(depends, c));
    }

    // Write the job script to file
    final Path scriptPath = new Path(appDir, tracker.name + ".sh");
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

  public void init(String[] args) {
    if (args.length == 0 || args.length > 1) {
      LOG.fatal("Usage: <command> applicationDirectory");
      System.exit(1);
    }
    appDir = new Path(args[0]);
  }

  public void run() throws Exception {
    conf = new YarnConfiguration();

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

    rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
    rmClient.init(conf);
    rmClient.start();

    nmClient = NMClientAsync.createNMClientAsync(this);
    nmClient.init(conf);
    nmClient.start();

    startServer();

    rmClient.registerApplicationMaster(hostname, port, "");

    intializeServices();

    server.awaitTermination();
  }

  private void shutdown() {
    // wait for completion.
    nmClient.stop();

    // Delete the app directory
    ugi.doAs(
        new PrivilegedAction<Void>() {
          public Void run() {
            try {
              FileSystem fs = FileSystem.get(conf);
              fs.delete(appDir, true);
              LOG.info("Deleted application directory " + appDir);
            } catch (IOException exc) {
              LOG.warn("Failed to delete application directory " + appDir, exc);
            }
            return null;
          }
        });

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

    stopServer();
  }

  /** Main entrypoint for the ApplicationMaster. **/
  public static void main(String[] args) {
    ApplicationMaster appMaster = new ApplicationMaster();

    appMaster.init(args);

    try {
      appMaster.run();
    } catch (Throwable exc) {
      fatal("Error running ApplicationMaster", exc);
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

  class MasterImpl extends MasterGrpc.MasterImplBase {
    @Override
    public void getJob(Msg.Empty req, StreamObserver<Msg.Job> resp) {
      resp.onNext(MsgUtils.writeJob(job));
      resp.onCompleted();
    }

    @Override
    public void getService(Msg.ServiceRequest req,
        StreamObserver<Msg.Service> resp) {
      String name = req.getName();
      Model.Service service = job.getServices().get(name);

      if (service == null) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Unknown Service '" + name + "'")
            .asRuntimeException());
        return;
      }
      resp.onNext(MsgUtils.writeService(service));
      resp.onCompleted();
    }
  }
}
