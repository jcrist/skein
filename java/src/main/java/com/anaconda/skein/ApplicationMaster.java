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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

public class ApplicationMaster {

  private static final Logger LOG = LogManager.getLogger(ApplicationMaster.class);

  // One for the boss, one for the worker. Should be fine under normal loads.
  private static final int NUM_EVENT_LOOP_GROUP_THREADS = 2;

  // The thread bounds for handling requests. Since we use locking at some
  // level, we can only get so much parallelism in *handling* requests.
  private static final int MIN_GRPC_EXECUTOR_THREADS = 2;
  private static final int MAX_GRPC_EXECUTOR_THREADS = 10;

  // The thread bounds for launching containers.
  private static final int MIN_EXECUTOR_THREADS = 0;
  private static final int MAX_EXECUTOR_THREADS = 25;

  private Configuration conf;

  private Model.ApplicationSpec spec;

  private Path appDir;

  private final ConcurrentHashMap<String, String> keyValueStore =
      new ConcurrentHashMap<String, String>();

  private final ConcurrentHashMap<String, List<StreamObserver<Msg.GetKeyResponse>>> alerts =
      new ConcurrentHashMap<String, List<StreamObserver<Msg.GetKeyResponse>>>();

  private final Map<String, ServiceTracker> services =
      new HashMap<String, ServiceTracker>();
  private final Map<ContainerId, Model.Container> containers =
      new ConcurrentHashMap<ContainerId, Model.Container>();

  private final TreeMap<Priority, ServiceTracker> priorities =
      new TreeMap<Priority, ServiceTracker>();
  private int nextPriority = 1;

  private Server server;
  private String hostname;
  private int port = -1;

  private AMRMClient<ContainerRequest> rmClient;
  private NMClient nmClient;
  private ThreadPoolExecutor executor;
  private Thread allocatorThread;
  private boolean shouldShutdown = false;

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

    NioEventLoopGroup eg = new NioEventLoopGroup(NUM_EVENT_LOOP_GROUP_THREADS);
    ThreadPoolExecutor executor = Utils.newThreadPoolExecutor(
        "grpc-executor",
        MIN_GRPC_EXECUTOR_THREADS,
        MAX_GRPC_EXECUTOR_THREADS,
        true);

    server = NettyServerBuilder.forPort(0)
        .sslContext(sslContext)
        .addService(new MasterImpl())
        .workerEventLoopGroup(eg)
        .bossEventLoopGroup(eg)
        .executor(executor)
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

  private void startAllocator() {
    allocatorThread =
      new Thread() {
        public void run() {
          while (!shouldShutdown) {
            try {
              long start = System.currentTimeMillis();
              allocate();
              long left = 1000 - (System.currentTimeMillis() - start);
              if (left > 0) {
                Thread.sleep(left);
              }
            } catch (InterruptedException exc) {
              break;
            } catch (Throwable exc) {
              shutdown(FinalApplicationStatus.FAILED,
                       exc.getMessage());
              break;
            }
          }
        }
      };
    allocatorThread.setDaemon(true);
    allocatorThread.start();
  }

  private void stopAllocator() {
    shouldShutdown = true;
    if (!Thread.currentThread().equals(allocatorThread)) {
      allocatorThread.interrupt();
    }
  }

  private void loadApplicationSpec() throws Exception {
    try {
      spec = MsgUtils.readApplicationSpec(
          Msg.ApplicationSpec.parseFrom(new FileInputStream(".skein.proto")));
    } catch (IOException exc) {
      fatal("Issue loading application specification", exc);
    }
    spec.validate();

    // Setup service trackers
    for (Map.Entry<String, Model.Service> entry : spec.getServices().entrySet()) {
      services.put(entry.getKey(),
          new ServiceTracker(entry.getKey(), entry.getValue()));
    }

    // Setup dependents
    for (ServiceTracker tracker : services.values()) {
      for (String dep : tracker.service.getDepends()) {
        services.get(dep).addDependent(tracker);
      }
    }

    LOG.info("Application specification successfully loaded");
  }

  // Due to how YARN container allocation works, sometimes duplicate requests
  // can be sent. This can be avoided if all outstanding requests are of a
  // different priority. To handle this, we keep a *sorted* map of all
  // outstanding priorities -> the service tracker that they are requesting. We
  // also keep an integer of the next priority to use to speed up allocating a
  // new ContainerRequest. This layout provides the following benefits:
  //
  // - Older requests get lower priorities. Since we always want FIFO for
  // requests, this makes sense.
  //
  // - The max priority is bounded by the maximum outstanding requests, meaning
  // for realistic workloads this should never grow unbounded.
  //
  // - We get O(1) lookup of returned requests. Until recently, returned
  // containers had no easy way to match them to their request - you had to
  // compare the resources and understand the rounding strategy. By always
  // matching priority -> request, this makes the pairing easy. As a safety
  // check, we also check that the resources match.
  private Priority newPriority(ServiceTracker tracker) {
    synchronized (priorities) {
      Priority priority = Priority.newInstance(nextPriority);
      nextPriority += 1;
      priorities.put(priority, tracker);
      return priority;
    }
  }

  private ServiceTracker trackerFromPriority(Priority priority) {
    synchronized (priorities) {
      return priorities.get(priority);
    }
  }

  private void removePriority(Priority priority) {
    synchronized (priorities) {
      priorities.remove(priority);
    }
  }

  private void updatePriorities() {
    // Store the next priority for fast access between allocation cycles.
    synchronized (priorities) {
      if (priorities.size() > 0) {
        nextPriority = priorities.lastKey().getPriority() + 1;
      } else {
        nextPriority = 1;
      }
    }
  }

  private void allocate() throws IOException, YarnException {
    // Since the application can dynamically allocate containers, we can't
    // accurately estimate the application progress. Set to started, but not
    // far along.
    AllocateResponse resp = rmClient.allocate(0.1f);

    List<Container> allocated = resp.getAllocatedContainers();
    List<ContainerStatus> completed = resp.getCompletedContainersStatuses();

    if (allocated.size() > 0) {
      handleAllocated(allocated);
    }

    if (completed.size() > 0) {
      handleCompleted(completed);
    }

    if (allocated.size() > 0 || completed.size() > 0) {
      updatePriorities();
    }
  }

  private void handleAllocated(List<Container> newContainers) {
    for (Container c : newContainers) {
      Priority priority = c.getPriority();
      Resource resource = c.getResource();

      ServiceTracker tracker = trackerFromPriority(priority);
      if (tracker != null && tracker.matches(resource)) {
        tracker.startContainer(c);
      } else {
        LOG.warn("No matching service found for resource: " + resource
                 + ", priority: " + priority + ", releasing " + c.getId());
        rmClient.releaseAssignedContainer(c.getId());
      }
    }
  }

  private void handleCompleted(List<ContainerStatus> containerStatuses) {
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

    loadApplicationSpec();

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

    rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    executor = Utils.newThreadPoolExecutor(
        "executor",
        MIN_EXECUTOR_THREADS,
        MAX_EXECUTOR_THREADS,
        true);

    startServer();

    rmClient.registerApplicationMaster(hostname, port, "");

    startAllocator();

    // Start services
    for (ServiceTracker tracker: services.values()) {
      tracker.initialize();
    }

    server.awaitTermination();
  }

  private void maybeShutdown() {
    // Fail if any service is failed
    // Succeed if all services are finished and none failed
    boolean finished = true;
    for (ServiceTracker tracker : services.values()) {
      finished &= tracker.isFinished();
      if (tracker.isFailed()) {
        shutdown(FinalApplicationStatus.FAILED,
                 "Failure in service " + tracker.getName()
                 + ", see logs for details");
        return;
      }
    }
    if (finished) {
      shutdown(FinalApplicationStatus.SUCCEEDED, "Completed Successfully");
    }
  }

  private void shutdown(FinalApplicationStatus status, String msg) {
    preshutdown(status, msg);

    System.exit(0);  // Trigger exit hooks
  }

  private void preshutdown(FinalApplicationStatus status, String msg) {
    try {
      rmClient.unregisterApplicationMaster(status, msg, null);
    } catch (Exception ex) {
      LOG.error("Failed to unregister application", ex);
    }
    stopAllocator();

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

  private final class ServiceTracker {
    private String name;
    private Model.Service service;
    private ContainerLaunchContext ctx;
    private boolean initialRunning = false;
    private final Set<String> depends = new HashSet<String>();
    private final Set<Integer> waiting = new LinkedHashSet<Integer>();
    // An ordered map of priority -> container. Earlier entries are older
    // requests. The priority is the same as container.req.getPriority().
    private final TreeMap<Priority, Model.Container> requested =
        new TreeMap<Priority, Model.Container>();
    private final Set<Integer> running = new LinkedHashSet<Integer>();
    private final List<Model.Container> containers = new ArrayList<Model.Container>();
    private final List<ServiceTracker> dependents = new ArrayList<ServiceTracker>();
    private int numTarget = 0;
    private int numSucceeded = 0;
    private int numFailed = 0;
    private int numKilled = 0;
    private int numRestarted = 0;

    public ServiceTracker(String name, Model.Service service) {
      this.name = name;
      this.service = service;
      this.depends.addAll(service.getDepends());
      this.numTarget = service.getInstances();
    }

    public boolean matches(Resource r) {
      return service.getResources().compareTo(r) <= 0;
    }

    public void addDependent(ServiceTracker tracker) {
      dependents.add(tracker);
    }

    public String getName() {
      return name;
    }

    public synchronized int getNumActive() {
      return waiting.size() + requested.size() + running.size();
    }

    private boolean isReady() {
      return depends.size() == 0;
    }

    public boolean isFinished() {
      return getNumActive() == 0;
    }

    public synchronized boolean isFailed() {
      return numFailed > numRestarted;
    }

    public synchronized void notifyRunning(String dependency) {
      depends.remove(dependency);
      if (isReady()) {
        for (int instance : waiting) {
          requestContainer(containers.get(instance));
        }
        waiting.clear();
      }
    }

    public synchronized void initialize() throws IOException {
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

    private synchronized Model.Container newContainer(Model.Container.State state) {
      Model.Container out = new Model.Container(name, containers.size(), state);
      containers.add(out);
      return out;
    }

    private synchronized Model.Container getContainer(int instance) {
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

    private synchronized void requestContainer(Model.Container container) {
      Priority priority = newPriority(this);
      ContainerRequest req = new ContainerRequest(service.getResources(),
                                                  null, null, priority);
      container.setContainerRequest(req);
      rmClient.addContainerRequest(req);
      requested.put(priority, container);
      LOG.info("REQUESTED: " + name + "_" + container.getInstance());
    }

    public synchronized Model.Container addContainer() {
      Model.Container container;
      if (!isReady()) {
        container = newContainer(Model.Container.State.WAITING);
        waiting.add(container.getInstance());
        LOG.info("WAITING: " + name + "_" + container.getInstance());
      } else {
        container = newContainer(Model.Container.State.REQUESTED);
        requestContainer(container);
      }
      return container;
    }

    private synchronized Model.Container removeContainer() {
      int instance;
      if (waiting.size() > 0) {
        instance = Utils.popfirst(waiting);
      } else if (requested.size() > 0) {
        instance = requested.get(requested.firstKey()).getInstance();
      } else {
        instance = Utils.popfirst(running);
      }
      finishContainer(instance, Model.Container.State.KILLED);
      return containers.get(instance);
    }

    public Model.Container startContainer(final Container container) {
      LOG.info("Starting " + container.getId());
      Priority priority = container.getPriority();
      removePriority(priority);

      Model.Container out;

      // Synchronize only in this block so that only one service is blocked at
      // a time (instead of potentially multiple).
      synchronized (this) {
        out = requested.remove(priority);
        final int instance = out.getInstance();
        // Remove request so it dosn't get resubmitted
        rmClient.removeContainerRequest(out.popContainerRequest());

        // Add fields for running container
        out.setState(Model.Container.State.RUNNING);
        out.setStartTime(System.currentTimeMillis());
        out.setYarnContainerId(container.getId());
        out.setYarnNodeId(container.getNodeId());

        ApplicationMaster.this.containers.put(container.getId(), out);
        running.add(instance);

        executor.execute(
            new Runnable() {
              @Override
              public void run() {
                try {
                  nmClient.startContainer(container, ServiceTracker.this.ctx);
                } catch (Throwable exc) {
                  LOG.warn("Failed to start " + ServiceTracker.this.name
                          + "_" + instance, exc);
                  ServiceTracker.this.finishContainer(instance,
                      Model.Container.State.FAILED);
                }
              }
            });

        LOG.info("RUNNING: " + name + "_" + instance + " on " + container.getId());
      }

      if (!initialRunning && requested.size() == 0) {
        initialRunning = true;
        for (ServiceTracker dep : dependents) {
          dep.notifyRunning(name);
        }
      }
      return out;
    }

    public synchronized void finishContainer(int instance,
        Model.Container.State state) {
      Model.Container container = containers.get(instance);

      switch (container.getState()) {
        case WAITING:
          waiting.remove(instance);
          break;
        case REQUESTED:
          ContainerRequest req = container.popContainerRequest();
          Priority priority = req.getPriority();
          removePriority(priority);
          requested.remove(priority);
          rmClient.removeContainerRequest(req);
          break;
        case RUNNING:
          rmClient.releaseAssignedContainer(container.getYarnContainerId());
          running.remove(instance);
          container.setFinishTime(System.currentTimeMillis());
          break;
        default:
          return;  // Already finished, should never get here
      }

      boolean mayRestart = false;
      switch (state) {
        case SUCCEEDED:
          numSucceeded += 1;
          break;
        case KILLED:
          numKilled += 1;
          break;
        case FAILED:
          numFailed += 1;
          mayRestart = true;
          break;
        default:
          throw new IllegalArgumentException(
              "finishContainer got illegal state " + state);
      }

      LOG.info(state + ": " + name + "_" + instance);
      container.setState(state);

      if (mayRestart && (service.getMaxRestarts() == -1
          || numRestarted < service.getMaxRestarts())) {
        numRestarted += 1;
        LOG.info("RESTARTING: adding new container to replace "
                 + name + "_" + instance);
        addContainer();
      }

      if (isFinished()) {
        maybeShutdown();
      }
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
    public void shutdown(Msg.ShutdownRequest req,
        StreamObserver<Msg.Empty> resp) {
      FinalApplicationStatus status = MsgUtils.readFinalStatus(req.getFinalStatus());

      // Shutdown everything but the grpc server
      preshutdown(status, "Shutdown by user");

      resp.onNext(MsgUtils.EMPTY);
      resp.onCompleted();

      // Finish shutdown
      System.exit(0);
    }

    @Override
    public void getApplicationSpec(Msg.Empty req, StreamObserver<Msg.ApplicationSpec> resp) {
      resp.onNext(MsgUtils.writeApplicationSpec(spec));
      resp.onCompleted();
    }

    @Override
    public void getService(Msg.ServiceRequest req,
        StreamObserver<Msg.Service> resp) {
      String name = req.getName();

      if (!checkService(name, resp)) {
        return;
      }
      Model.Service service = spec.getServices().get(name);
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
        ServiceTracker tracker = services.get(name);
        // Lock on tracker to prevent containers from updating while writing.
        // If this proves costly, may want to copy beforehand.
        synchronized (tracker) {
          for (Model.Container c : tracker.containers) {
            if (stateSet.contains(c.getState())) {
              msg.addContainers(MsgUtils.writeContainer(c));
            }
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
      tracker.finishContainer(instance, Model.Container.State.KILLED);
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

      Msg.ContainersResponse.Builder msg = Msg.ContainersResponse.newBuilder();
      ServiceTracker tracker = services.get(service);
      // Lock on tracker to prevent containers from updating while writing. If
      // this proves costly, may want to copy beforehand.
      synchronized (tracker) {
        for (Model.Container c : tracker.scale(instances)) {
          msg.addContainers(MsgUtils.writeContainer(c));
        }
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
