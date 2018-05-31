package com.anaconda.skein;

import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class ApplicationMaster implements AMRMClientAsync.CallbackHandler,
       NMClientAsync.CallbackHandler {

  private static final Logger LOG = LogManager.getLogger(ApplicationMaster.class);

  private Configuration conf;

  private Model.Job job;

  private Path appDir;

  private final ConcurrentHashMap<String, String> keyValueStore =
      new ConcurrentHashMap<String, String>();

  private final ConcurrentHashMap<String, List<StreamObserver<Msg.GetKeyResponse>>> alerts =
      new ConcurrentHashMap<String, List<StreamObserver<Msg.GetKeyResponse>>>();

  private final List<ServiceTracker> trackers =
      new ArrayList<ServiceTracker>();
  private final Map<String, ServiceTracker> services =
      new HashMap<String, ServiceTracker>();
  private final Map<ContainerId, Model.Container> containers =
      new HashMap<ContainerId, Model.Container>();

  private Server server;
  private String hostname;
  private int port = -1;

  private AMRMClientAsync rmClient;
  private NMClientAsync nmClient;
  private UserGroupInformation ugi;
  private ByteBuffer tokens;

  private void startServer() throws IOException {
    // Setup and start the server
    SslContext sslContext = GrpcSslContexts
        .forServer(new File(".skein.crt"), new File(".skein.pem"))
        .trustManager(new File(".skein.crt"))
        .clientAuth(ClientAuth.REQUIRE)
        .sslProvider(SslProvider.OPENSSL)
        .build();

    server = NettyServerBuilder.forPort(0)
        .sslContext(sslContext)
        .addService(new MasterImpl())
        .executor(Executors.newSingleThreadExecutor())
        .build()
        .start();

    port = server.getPort();

    LOG.info("Server started, listening on " + port);

    Runtime.getRuntime().addShutdownHook(
        new Thread() {
          @Override
          public void run() {
            ApplicationMaster.this.stopServer();
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

  private void loadJob() throws Exception {
    try {
      job = MsgUtils.readJob(Msg.Job.parseFrom(new FileInputStream(".skein.proto")));
    } catch (IOException exc) {
      fatal("Issue loading job specification", exc);
    }
    job.validate();
    LOG.info("Job successfully loaded");
  }

  private synchronized void intializeServices() throws Exception {
    for (Map.Entry<String, Model.Service> entry : job.getServices().entrySet()) {
      String serviceName = entry.getKey();
      Model.Service service = entry.getValue();

      ServiceTracker tracker = new ServiceTracker(serviceName, service);

      services.put(serviceName, tracker);
      trackers.add(tracker);
    }

    // Setup dependents
    for (ServiceTracker tracker : trackers) {
      for (String dep : tracker.service.getDepends()) {
        services.get(dep).addDependent(tracker);
      }
    }

    // Sort trackers by resources
    Collections.sort(trackers);

    // Startup services
    for (ServiceTracker tracker: trackers) {
      tracker.initialize();
    }
  }

  /* ResourceManager Callbacks */

  @Override
  public synchronized void onContainersCompleted(List<ContainerStatus> containerStatuses) {
    for (ContainerStatus status : containerStatuses) {
      Model.Container container = containers.get(status.getContainerId());
      if (container == null) {
        continue;  // release container that was never started
      }

      Model.Container.State state;
      int exitStatus = status.getExitStatus();

      if (exitStatus == ContainerExitStatus.SUCCESS) {
        state = Model.Container.State.SUCCEEDED;
      } else if (exitStatus == ContainerExitStatus.KILLED_BY_APPMASTER) {
        return;  // state change already handled by killContainer
      } else {
        state = Model.Container.State.FAILED;
      }

      services.get(container.getServiceName())
              .finishContainer(container.getInstance(), state);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized void onContainersAllocated(List<Container> newContainers) {
    for (Container c : newContainers) {
      boolean found = false;
      for (ServiceTracker t : trackers) {
        if (t.matches(c.getResource())) {
          found = true;
          t.startContainer(c);
          break;
        }
      }
      if (!found) {
        // TODO: For some reason YARN allocates extra containers *sometimes*.
        // It's not clear yet if this is a bug in skein or in YARN, for now
        // release extra containers and log the discrepancy.
        LOG.warn("No matching service found for resource: " + c.getResource()
                 + " releasing " + c.getId());
        rmClient.releaseAssignedContainer(c.getId());
      }
    }
  }

  @Override
  public void onShutdownRequest() {
    shutdown(FinalApplicationStatus.SUCCEEDED, "Shutdown Requested");
  }

  @Override
  public void onError(Throwable exc) {
    shutdown(FinalApplicationStatus.FAILED, exc.toString());
  }

  @Override
  public void onNodesUpdated(List<NodeReport> nodeReports) {}

  @Override
  public float getProgress() {
    float completed = 0;
    float total = 0;
    for (ServiceTracker tracker : trackers) {
      completed += tracker.getNumCompleted();
      total += tracker.getNumTotal();
    }
    return completed / total;
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
    Model.Container container = containers.get(containerId);
    services.get(container.getServiceName())
            .finishContainer(container.getInstance(),
                             Model.Container.State.FAILED);
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

    loadJob();

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

  private void maybeShutdown() {
    FinalApplicationStatus status = FinalApplicationStatus.SUCCEEDED;
    boolean finished = true;
    String msg = "Completed Successfully";
    for (ServiceTracker tracker : trackers) {
      finished &= tracker.isFinished();
      if (tracker.hasFailed()) {
        status = FinalApplicationStatus.FAILED;
        msg = ("Failure in service " + tracker.getName() + ", shutting down. "
               + "See logs for details");
        break;
      }
    }
    if (finished) {
      shutdown(status, msg);
    }
  }

  private void shutdown(FinalApplicationStatus status, String msg) {
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

    try {
      rmClient.unregisterApplicationMaster(status, msg, null);
    } catch (Exception ex) {
      LOG.error("Failed to unregister application", ex);
    }

    rmClient.stop();
    System.exit(0);  // Trigger exit hooks
  }

  /** Main entrypoint for the ApplicationMaster. **/
  public static void main(String[] args) {
    // Specify the netty native workdir. This is necessary for systems where
    // `/tmp` is not executable.
    System.setProperty("io.netty.native.workdir", "./");

    ApplicationMaster appMaster = new ApplicationMaster();

    appMaster.init(args);

    try {
      appMaster.run();
    } catch (Throwable exc) {
      fatal("Error running ApplicationMaster", exc);
    }
  }

  private final class ServiceTracker implements Comparable<ServiceTracker> {
    private String name;
    private Model.Service service;
    private ContainerLaunchContext ctx;
    private boolean initialRunning = false;
    private final Set<String> depends = new HashSet<String>();
    private final Set<Integer> waiting = new LinkedHashSet<Integer>();
    private final Set<Integer> requested = new LinkedHashSet<Integer>();
    private final Set<Integer> running = new LinkedHashSet<Integer>();
    private final List<Model.Container> containers = new ArrayList<Model.Container>();
    private final List<ServiceTracker> dependents = new ArrayList<ServiceTracker>();
    private int numTarget = 0;
    private int numSucceeded = 0;
    private int numFailed = 0;
    private int numKilled = 0;

    public ServiceTracker(String name, Model.Service service) {
      this.name = name;
      this.service = service;
      this.depends.addAll(service.getDepends());
      this.numTarget = service.getInstances();
    }

    public boolean matches(Resource r) {
      // requested and requirement <= response
      return requested.size() > 0 && service.getResources().compareTo(r) <= 0;
    }

    public int compareTo(ServiceTracker other) {
      return service.getResources().compareTo(other.service.getResources());
    }

    public void addDependent(ServiceTracker tracker) {
      dependents.add(tracker);
    }

    public String getName() {
      return name;
    }

    public int getNumTotal() {
      return containers.size();
    }

    public int getNumCompleted() {
      return numSucceeded + numFailed + numKilled;
    }

    public int getNumActive() {
      return getNumTotal() - getNumCompleted();
    }

    private boolean isReady() {
      return depends.size() == 0;
    }

    public boolean isFinished() {
      return numFailed > 0 || numSucceeded + numKilled == numTarget;
    }

    public boolean hasFailed() {
      return numFailed > 0;
    }

    @SuppressWarnings("unchecked")
    public void notifyRunning(String dependency) {
      depends.remove(dependency);
      if (isReady()) {
        for (int instance : waiting) {
          rmClient.addContainerRequest(
              new ContainerRequest(service.getResources(),
                                   null, null, Priority.newInstance(0)));
          requested.add(instance);
          containers.get(instance).setState(Model.Container.State.REQUESTED);
          LOG.info("REQUESTED: " + name + "_" + instance);
        }
        waiting.clear();
      }
    }

    public void initialize() throws IOException {
      LOG.info("INTIALIZING: " + name);
      // Add appmaster address to environment
      service.getEnv().put("SKEIN_APPMASTER_ADDRESS", hostname + ":" + port);

      ctx = ContainerLaunchContext.newInstance(
          service.getLocalResources(), service.getEnv(), service.getCommands(),
          null, tokens, null);

      // Request initial containers
      for (int i = 0; i < service.getInstances(); i++) {
        addContainer();
      }
    }

    private Model.Container newContainer(Model.Container.State state) {
      Model.Container out = new Model.Container(name, containers.size(), state);
      containers.add(out);
      LOG.info(state + ": " + name + "_" + out.getInstance());
      return out;
    }

    private Model.Container getContainer(int instance) {
      if (instance >= 0 && instance < containers.size()) {
        return containers.get(instance);
      }
      return null;
    }

    public synchronized List<Model.Container> scale(int instances) {
      List<Model.Container> out =  new ArrayList<Model.Container>();

      int active = getNumActive();
      int delta = instances - active;
      LOG.info("Scaling service '" + name + "' to " + instances
               + " instances, a delta of " + delta);
      if (delta > 0) {
        // Scale up
        for (int i = 0; i < delta; i++) {
          out.add(addContainer());
          numTarget += 1;
        }
      } else if (delta < 0) {
        // Scale down
        for (int i = delta; i < 0; i++) {
          out.add(removeContainer());
        }
      }
      return out;
    }

    @SuppressWarnings("unchecked")
    public Model.Container addContainer() {
      Model.Container container;
      if (!isReady()) {
        container = newContainer(Model.Container.State.WAITING);
        waiting.add(container.getInstance());
      } else {
        rmClient.addContainerRequest(
            new ContainerRequest(service.getResources(),
                                 null, null, Priority.newInstance(0)));
        container = newContainer(Model.Container.State.REQUESTED);
        requested.add(container.getInstance());
      }
      return container;
    }

    private Model.Container removeContainer() {
      int instance;
      if (waiting.size() > 0) {
        instance = Utils.popfirst(waiting);
      } else if (requested.size() > 0) {
        instance = Utils.popfirst(requested);
      } else {
        instance = Utils.popfirst(running);
      }
      killContainer(instance);
      return containers.get(instance);
    }

    public synchronized void killContainer(int instance) {
      Model.Container container = containers.get(instance);
      if (container.getState() == Model.Container.State.RUNNING) {
        nmClient.stopContainerAsync(container.getYarnContainerId(),
                                    container.getYarnNodeId());
      }
      finishContainer(instance, Model.Container.State.KILLED);
    }

    public Model.Container startContainer(Container container) {
      int instance = Utils.popfirst(requested);
      Model.Container out = containers.get(instance);

      // Add fields for running container
      out.setState(Model.Container.State.RUNNING);
      out.setStartTime(System.currentTimeMillis());
      out.setYarnContainerId(container.getId());
      out.setYarnNodeId(container.getNodeId());
      running.add(instance);

      ApplicationMaster.this.containers.put(container.getId(), out);

      nmClient.startContainerAsync(container, ctx);

      LOG.info("RUNNING: " + name + "_" + instance + " on " + container.getId());

      if (!initialRunning && requested.size() == 0) {
        initialRunning = true;
        for (ServiceTracker dep : dependents) {
          dep.notifyRunning(name);
        }
      }
      return out;
    }

    public void finishContainer(int instance, Model.Container.State state) {
      Model.Container container = containers.get(instance);

      switch (container.getState()) {
        case WAITING:
          waiting.remove(instance);
          break;
        case REQUESTED:
          requested.remove(instance);
          break;
        case RUNNING:
          running.remove(instance);
          container.setFinishTime(System.currentTimeMillis());
          break;
        default:
          return;  // Already finished, should never get here
      }

      if (state == Model.Container.State.SUCCEEDED) {
        numSucceeded += 1;
      } else if (state == Model.Container.State.KILLED) {
        numKilled += 1;
      } else {
        numFailed += 1;
      }

      LOG.info(state + ": " + name + "_" + instance);
      container.setState(state);

      maybeShutdown();
    }
  }

  class MasterImpl extends MasterGrpc.MasterImplBase {

    private boolean checkService(String name, StreamObserver<?> resp) {
      if (services.get(name) == null) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Unknown Service '" + name + "'")
            .asRuntimeException());
        return false;
      }
      return true;
    }

    @Override
    public void getJob(Msg.Empty req, StreamObserver<Msg.Job> resp) {
      resp.onNext(MsgUtils.writeJob(job));
      resp.onCompleted();
    }

    @Override
    public void getService(Msg.ServiceRequest req,
        StreamObserver<Msg.Service> resp) {
      String name = req.getName();

      if (!checkService(name, resp)) {
        return;
      }
      Model.Service service = job.getServices().get(name);
      resp.onNext(MsgUtils.writeService(service));
      resp.onCompleted();
    }

    @Override
    public void getContainers(Msg.ContainersRequest req,
        StreamObserver<Msg.ContainersResponse> resp) {

      Set<String> serviceSet;
      if (req.getServicesCount() == 0) {
        serviceSet = services.keySet();
      } else {
        serviceSet = new HashSet<String>(req.getServicesList());
        for (String name : serviceSet) {
          if (!checkService(name, resp)) {
            return;
          }
        }
      }

      Set<Model.Container.State> stateSet;
      if (req.getStatesCount() > 0) {
        stateSet = EnumSet.noneOf(Model.Container.State.class);
        for (Msg.Container.State s : req.getStatesList()) {
          stateSet.add(MsgUtils.readContainerState(s));
        }
      } else {
        stateSet = EnumSet.of(Model.Container.State.WAITING,
                              Model.Container.State.REQUESTED,
                              Model.Container.State.RUNNING);
      }

      // Filter containers and build response
      Msg.ContainersResponse.Builder msg = Msg.ContainersResponse.newBuilder();
      for (String name : serviceSet) {
        for (Model.Container c : services.get(name).containers) {
          if (stateSet.contains(c.getState())) {
            msg.addContainers(MsgUtils.writeContainer(c));
          }
        }
      }
      resp.onNext(msg.build());
      resp.onCompleted();
    }

    @Override
    public void killContainer(Msg.ContainerInstance req,
        StreamObserver<Msg.Empty> resp) {

      String service = req.getServiceName();
      int instance = req.getInstance();
      if (!checkService(service, resp)) {
        return;
      }

      ServiceTracker tracker = services.get(service);
      if (tracker.getContainer(instance) == null) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Service '" + service + "' has no container "
                             + "instance " + instance)
            .asRuntimeException());
        return;
      }
      tracker.killContainer(instance);
      resp.onNext(MsgUtils.EMPTY);
      resp.onCompleted();
    }

    @Override
    public void scale(Msg.ScaleRequest req,
        StreamObserver<Msg.ContainersResponse> resp) {

      String service = req.getServiceName();
      if (!checkService(service, resp)) {
        return;
      }

      int instances = req.getInstances();
      if (instances < 0) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("instances must be >= 0")
            .asRuntimeException());
        return;
      }

      ServiceTracker tracker = services.get(service);
      List<Model.Container> changes = tracker.scale(instances);

      Msg.ContainersResponse.Builder msg = Msg.ContainersResponse.newBuilder();
      for (Model.Container c : changes) {
        msg.addContainers(MsgUtils.writeContainer(c));
      }
      resp.onNext(msg.build());
      resp.onCompleted();
    }

    @Override
    public void keyvalueGetKey(Msg.GetKeyRequest req,
        StreamObserver<Msg.GetKeyResponse> resp) {
      String key = req.getKey();
      String val = keyValueStore.get(key);
      if (val == null) {
        if (req.getWait()) {
          if (alerts.get(key) == null) {
            alerts.put(key, new ArrayList<StreamObserver<Msg.GetKeyResponse>>());
          }
          alerts.get(key).add(resp);
        } else {
          resp.onError(Status.NOT_FOUND
              .withDescription(key)
              .asRuntimeException());
        }
      } else {
        resp.onNext(Msg.GetKeyResponse.newBuilder().setVal(val).build());
        resp.onCompleted();
      }
    }

    @Override
    public void keyvalueSetKey(Msg.SetKeyRequest req,
        StreamObserver<Msg.Empty> resp) {
      String key = req.getKey();
      String val = req.getVal();

      keyValueStore.put(key, val);

      resp.onNext(MsgUtils.EMPTY);
      resp.onCompleted();

      // Handle alerts
      List<StreamObserver<Msg.GetKeyResponse>> callbacks = alerts.get(key);
      if (callbacks != null) {
        Msg.GetKeyResponse cbResp =
            Msg.GetKeyResponse.newBuilder().setVal(val).build();
        for (StreamObserver<Msg.GetKeyResponse> cb : callbacks) {
          try {
            cb.onNext(cbResp);
            cb.onCompleted();
          } catch (StatusRuntimeException exc) {
            if (exc.getStatus().getCode() != Status.Code.CANCELLED) {
              LOG.info("Callback failed on key: " + key
                       + ", status: " + exc.getStatus());
            }
          }
        }
        alerts.remove(key);
      }
    }

    @Override
    public void keyvalueDelKey(Msg.DelKeyRequest req, StreamObserver<Msg.Empty> resp) {
      String key = req.getKey();
      if (keyValueStore.remove(key) == null) {
        resp.onError(Status.NOT_FOUND.withDescription(key).asRuntimeException());
      } else {
        resp.onNext(MsgUtils.EMPTY);
        resp.onCompleted();
      }
    }

    @Override
    public void keyvalueGetAll(Msg.Empty req,
        StreamObserver<Msg.KeyValueResponse> resp) {
      resp.onNext(Msg.KeyValueResponse
          .newBuilder()
          .putAllItems(keyValueStore)
          .build());
      resp.onCompleted();
    }
  }
}
