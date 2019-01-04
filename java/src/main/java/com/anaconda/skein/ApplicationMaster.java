package com.anaconda.skein;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMaster {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMaster.class);

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

  private ApplicationId appId;
  private String userName;
  private ContainerId containerId;
  private Resource amResources;

  private final TreeMap<String, Msg.KeyValue.Builder> keyValueStore =
      new TreeMap<String, Msg.KeyValue.Builder>();
  private final IntervalTree<Watcher> intervalTree = new IntervalTree<Watcher>();

  private final Map<String, ServiceTracker> services =
      new HashMap<String, ServiceTracker>();
  private final Map<ContainerId, Model.Container> containers =
      new ConcurrentHashMap<ContainerId, Model.Container>();

  // Set to negative to indicate hasn't been set by user
  private final AtomicDouble progress = new AtomicDouble(-1);
  private final AtomicDouble totalMemory = new AtomicDouble(0);
  private final AtomicInteger totalVcores = new AtomicInteger(0);
  private final long startTimeMillis = System.currentTimeMillis();

  private final TreeMap<Priority, ServiceTracker> priorities =
      new TreeMap<Priority, ServiceTracker>();
  private int nextPriority = 1;

  private Server grpcServer;
  private WebUI ui;
  private String hostname;

  private FileSystem fs;
  private AMRMClient<ContainerRequest> rmClient;
  private NMClient nmClient;
  private ThreadPoolExecutor executor;
  private Thread allocatorThread;
  private Process driverProcess;
  private Thread driverThread;

  // Flags to communicate with exit hook
  // - The defaults will only be used in the case of an unexpected shutdown
  //   of the application master.
  boolean currentlyRegistered = false;
  FinalApplicationStatus finalStatus = FinalApplicationStatus.FAILED;
  String finalMessage = ("Application master failed unexpectedly. See the "
                         + "logs for more information.");

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

    grpcServer = NettyServerBuilder.forPort(0)
        .sslContext(sslContext)
        .addService(new AppMasterImpl())
        .workerEventLoopGroup(eg)
        .bossEventLoopGroup(eg)
        .executor(executor)
        .build()
        .start();

    LOG.info("gRPC server started at {}:{}", hostname, grpcServer.getPort());
  }

  private void stopServer() {
    if (grpcServer != null) {
      grpcServer.shutdown();
      LOG.info("gRPC server shut down");
    }
  }

  private void startUI() {
    // Sorted list of service trackers
    final List<ServiceTracker> sortedServices = Lists.newArrayList(services.values());
    Collections.sort(sortedServices, new Comparator<ServiceTracker>() {
      public int compare(ServiceTracker x, ServiceTracker y) {
        return x.getName().compareTo(y.getName());
      }
    });

    List<WebUI.ServiceContext> serviceContexts = Lists.transform(sortedServices,
        new Function<ServiceTracker, WebUI.ServiceContext>() {
          public WebUI.ServiceContext apply(ServiceTracker tracker) {
            return tracker.toServiceContext();
          }
        }
    );

    // Forward restricted set of users to the WebUI if:
    // - ACLs are enabled
    // - The wildcard * is not in the ui acl
    Set<String> allowedUsers = null;
    Model.Acls acls = spec.getAcls();
    if (acls.getEnable()) {
      List<String> uiUsers = acls.getUiUsers();
      if (!uiUsers.contains("*")) {
        allowedUsers = new HashSet<String>(uiUsers);
        // The application owner is always allowed
        allowedUsers.add(ugi.getShortUserName());
      }
    }


    try {
      boolean hasDriver = !spec.getMaster().getScript().isEmpty();
      String amLogAddress = WebAppUtils.getRunningLogURL(
          hostname + ":" + System.getenv(Environment.NM_HTTP_PORT.name()),
          containerId.toString(),
          System.getenv(Environment.USER.name())
      );
      ui = new WebUI(0, appId.toString(), spec.getName(), userName,
                     amLogAddress, hasDriver, progress, totalMemory,
                     totalVcores, startTimeMillis, keyValueStore,
                     serviceContexts, allowedUsers, conf, false);

      ui.start();
    } catch (Exception e) {
      fatal("Failed to start WebUI server", e);
    }

    LOG.info("WebUI server started at {}:{}", hostname, ui.getURI().getPort());
  }

  private void stopUI() {
    if (ui != null) {
      ui.stop();
      LOG.info("WebUI server shut down");
    }
  }

  private void startAllocator() {
    allocatorThread =
      new Thread() {
        public void run() {
          while (true) {
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
    if (!Thread.currentThread().equals(allocatorThread)) {
      allocatorThread.interrupt();
    }
  }

  private void startApplicationDriver() throws IOException {
    Model.Master master = spec.getMaster();
    if (master.getScript().isEmpty()) {
      // Nothing to do
      return;
    }
    LOG.debug("Writing driver script...");
    Utils.stringToFile(master.getScript(), new FileOutputStream(".skein.sh"));
    String logdir = System.getProperty("skein.log.directory");
    ProcessBuilder pb = new ProcessBuilder()
        .command("bash", ".skein.sh")
        .redirectErrorStream(true)
        .redirectOutput(new File(logdir, "application.driver.log"));
    updateServiceEnvironment(pb.environment(), amResources, null);
    pb.environment().remove("CLASSPATH");

    // Start the driver process
    LOG.info("Starting application driver");
    driverProcess = pb.start();

    // Set up a thread to manage the driver process
    driverThread =
      new Thread() {
        public void run() {
          // Wait for it to finish
          int exitValue;
          try {
            exitValue = driverProcess.waitFor();
          } catch (InterruptedException exc) {
            // Interrupted during some other shutdown process, ignore
            return;
          }
          if (exitValue == 0) {
            shutdown(FinalApplicationStatus.SUCCEEDED,
                     "Application driver completed successfully.");
          } else if (exitValue == 143) {
            // SIGTERM results in exit code 143
            shutdown(FinalApplicationStatus.FAILED,
                     "Application driver failed with exit code 143. "
                     + "This is often due to the application master "
                     + "memory limit being exceeded. See the "
                     + "diagnostics for more information.");
          } else {
            shutdown(FinalApplicationStatus.FAILED,
                     "Application driver failed with exit code "
                     + exitValue + ", see logs for more information.");
          }
        }
      };
    driverThread.setDaemon(true);
    driverThread.start();
  }

  private void stopApplicationDriver() {
    if (driverThread != null && driverThread.isAlive()) {
      LOG.info("Stopping application driver");
      driverThread.interrupt();
      driverProcess.destroy();
    }
  }

  private void updateServiceEnvironment(Map<String, String> env, Resource resource,
      String containerId) {
    env.put("SKEIN_APPMASTER_ADDRESS", hostname + ":" + grpcServer.getPort());
    env.put("SKEIN_APPLICATION_ID", appId.toString());
    env.put("SKEIN_RESOURCE_VCORES", String.valueOf(resource.getVirtualCores()));
    env.put("SKEIN_RESOURCE_MEMORY", String.valueOf(resource.getMemory()));
    if (containerId != null) {
      env.put("SKEIN_CONTAINER_ID", containerId);
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
    // If the user hasn't set the progress, set it to started but not far along.
    float prog = progress.floatValue();
    AllocateResponse resp = rmClient.allocate(prog < 0 ? 0.1f : prog);

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
    LOG.debug("Received {} new containers", newContainers.size());

    for (Container c : newContainers) {
      Priority priority = c.getPriority();
      Resource resource = c.getResource();

      ServiceTracker tracker = trackerFromPriority(priority);
      if (tracker != null && tracker.matches(resource)) {
        tracker.startContainer(c, resource);
      } else {
        LOG.warn("No matching service found for resource {}, priority {}, releasing {}",
                 resource, priority, c.getId());
        rmClient.releaseAssignedContainer(c.getId());
      }
    }
  }

  private void handleCompleted(List<ContainerStatus> containerStatuses) {
    LOG.debug("Received {} completed containers", containerStatuses.size());

    for (ContainerStatus status : containerStatuses) {
      Model.Container container = containers.get(status.getContainerId());
      if (container == null) {
        // released container that was never started
        LOG.debug("Releasing newly allocated container {} due to canceled request",
                  status.getContainerId());
        continue;
      }

      Model.Container.State state;
      String exitMessage;

      switch (status.getExitStatus()) {
        case ContainerExitStatus.KILLED_BY_APPMASTER:
          return;  // state change already handled by killContainer
        case ContainerExitStatus.SUCCESS:
          state = Model.Container.State.SUCCEEDED;
          exitMessage = "Completed successfully.";
          break;
        case ContainerExitStatus.KILLED_EXCEEDED_PMEM:
          state = Model.Container.State.FAILED;
          exitMessage = Utils.formatExceededMemMessage(
              status.getDiagnostics(),
              Utils.EXCEEDED_PMEM_PATTERN);
          break;
        case ContainerExitStatus.KILLED_EXCEEDED_VMEM:
          state = Model.Container.State.FAILED;
          exitMessage = Utils.formatExceededMemMessage(
              status.getDiagnostics(),
              Utils.EXCEEDED_VMEM_PATTERN);
          break;
        default:
          state = Model.Container.State.FAILED;
          if (status.getExitStatus() > 0) {
            // Positive error codes indicate service failure
            exitMessage = "Container failed during execution, see logs for more information.";
          } else {
            exitMessage = status.getDiagnostics();
          }
          break;
      }

      services.get(container.getServiceName())
              .finishContainer(container.getInstance(), state, exitMessage);
    }
  }

  private static void fatal(String msg, Throwable exc) {
    LOG.error(msg, exc);
    System.exit(1);
  }

  public void init(String[] args) {
    if (args.length != 1) {
      LOG.error("Usage: <command> applicationDirectory");
      System.exit(1);
    }
    appDir = new Path(args[0]);
    String appIdEnv = System.getenv("SKEIN_APPLICATION_ID");
    if (appIdEnv == null) {
      LOG.error("Couldn't find 'SKEIN_APPLICATION_ID' envar");
      System.exit(1);
    }
    appId = Utils.appIdFromString(appIdEnv);

    containerId = ConverterUtils.toContainerId(
        System.getenv(Environment.CONTAINER_ID.name())
    );
  }

  public void run() throws Exception {
    conf = new YarnConfiguration();

    // Initialize HDFS client
    fs = FileSystem.get(conf);

    // Register this hook early so it always gets run
    // - We register with a higher priority than FileSystem
    //   to ensure that the hdfs client is still active
    ShutdownHookManager.get().addShutdownHook(
        new Runnable() {
          @Override
          public void run() {
            runOnExit();
          }
        },
        FileSystem.SHUTDOWN_HOOK_PRIORITY + 30);

    loadApplicationSpec();

    // Determine application username
    userName = System.getenv(Environment.USER.name());
    LOG.info("Running as user {}", userName);

    // Remove the AM->RM token
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Create ugi and add original tokens to it
    ugi = UserGroupInformation.createRemoteUser(userName);
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

    hostname = System.getenv(Environment.NM_HOST.name());
    startServer();
    startUI();

    synchronized (this) {
      rmClient.registerApplicationMaster(
          hostname, grpcServer.getPort(), ui.getURI().toString()
      );
      currentlyRegistered = true;
    }

    startAllocator();

    // Determine the actual memory/vcores allocated to *this* container. We
    // need to do this here, after the application is already registered
    loadAppMasterResources();
    totalMemory.addAndGet(amResources.getMemory());
    totalVcores.addAndGet(amResources.getVirtualCores());

    // Start application driver (if applicable)
    startApplicationDriver();

    // Start services
    for (ServiceTracker tracker: services.values()) {
      tracker.initialize();
    }

    grpcServer.awaitTermination();
  }

  private void maybeShutdown() {
    // Fail if any service is failed
    // Succeed if no driver, all services are finished, and none failed
    boolean finished = (driverProcess == null);
    for (ServiceTracker tracker : services.values()) {
      finished &= tracker.isFinished();
      if (tracker.isFailed()) {
        shutdown(FinalApplicationStatus.FAILED,
                 "Failure in service " + tracker.getName()
                 + ", see logs for more information.");
        return;
      }
    }
    if (finished) {
      shutdown(FinalApplicationStatus.SUCCEEDED,
               "Application completed successfully.");
    }
  }

  private void shutdown(FinalApplicationStatus status, String msg) {
    preshutdown(status, msg);
    System.exit(0);  // Trigger exit hooks
  }

  private void preshutdown(FinalApplicationStatus status, String msg) {
    LOG.info("Shutting down: {}", msg);
    finalStatus = status;
    finalMessage = msg;
  }

  private void loadAppMasterResources() throws IOException, YarnException {
    LOG.debug("Determining resources available for application master");
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    amResources = yarnClient
      .getApplicationReport(appId)
      .getApplicationResourceUsageReport()
      .getUsedResources();
    yarnClient.stop();
  }

  private int getMaxAttempts() {
    int maxAttempts = spec.getMaxAttempts();
    int yarnMaxAttempts = conf.getInt(
        YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS
    );
    if (yarnMaxAttempts < maxAttempts) {
      maxAttempts = yarnMaxAttempts;
    }
    return maxAttempts;
  }

  private int getCurrentAttempt() {
    return containerId.getApplicationAttemptId().getAttemptId();
  }

  private void runOnExit() {
    int maxAttempts = getMaxAttempts();
    int currentAttempt = getCurrentAttempt();
    boolean isLastAttempt = currentAttempt >= maxAttempts;

    if (finalStatus == FinalApplicationStatus.SUCCEEDED || isLastAttempt) {
      // Unregister the application
      synchronized (this) {
        if (currentlyRegistered) {
          try {
            LOG.info("Unregistering application with status {}", finalStatus);
            rmClient.unregisterApplicationMaster(finalStatus, finalMessage, null);
            currentlyRegistered = false;
          } catch (Exception ex) {
            LOG.error("Failed to unregister application", ex);
          }
        }
      }
      // Delete the app directory
      try {
        if (fs.delete(appDir, true)) {
          LOG.info("Deleted application directory {}", appDir);
        }
      } catch (IOException exc) {
        LOG.warn("Failed to delete application directory {}", appDir, exc);
      }
    } else {
      LOG.info("Application attempt {} out of {} failed, will retry",
               currentAttempt, maxAttempts);
    }

    stopApplicationDriver();
    stopAllocator();
    stopUI();
    stopServer();
  }

  /** Main entrypoint for the ApplicationMaster. **/
  public static void main(String[] args) {
    // Specify the netty native workdir. This is necessary for systems where
    // `/tmp` is not executable.
    Utils.configureNettyNativeWorkDir();

    ApplicationMaster appMaster = new ApplicationMaster();

    appMaster.init(args);

    try {
      appMaster.run();
    } catch (Throwable exc) {
      fatal("Error running ApplicationMaster", exc);
    }
  }

  private final class WatchRequestStream implements StreamObserver<Msg.WatchRequest> {
    private StreamObserver<Msg.WatchResponse> resp;
    private final Set<Integer> registered = new HashSet<Integer>();

    WatchRequestStream(StreamObserver<Msg.WatchResponse> resp) {
      super();
      this.resp = resp;
      LOG.debug("New watch stream created [stream: {}]",
                System.identityHashCode(this));
    }

    private void removeWatch(int watchId) {
      if (registered.remove(watchId)) {
        synchronized (keyValueStore) {
          intervalTree.remove(watchId);
        }
        LOG.debug("Removed watcher [stream: {}, watcher: {}]",
                  System.identityHashCode(this), watchId);
      }
    }

    private void removeAllWatches() {
      synchronized (keyValueStore) {
        for (Iterator<Integer> it = registered.iterator(); it.hasNext();) {
          int watchId = it.next();
          intervalTree.remove(watchId);
          LOG.debug("Removed watcher [stream: {}, watcher: {}]",
                    System.identityHashCode(this), watchId);
          it.remove();
        }
      }
    }

    private boolean isActive() { return registered.size() > 0; }

    @Override
    public void onNext(Msg.WatchRequest req) {
      Msg.WatchResponse.Builder builder = Msg.WatchResponse.newBuilder();
      int watchId;
      switch (req.getRequestCase()) {
        case CREATE:
          Msg.WatchCreateRequest create = req.getCreate();
          String start = create.getStart();
          String end = create.getEnd();
          Msg.WatchCreateRequest.Type type = create.getEventType();
          synchronized (keyValueStore) {
            watchId = intervalTree.add(start, end, new Watcher(resp, this, type));
          }
          LOG.debug("Created watcher [stream: {}, watcher: {}, start: '{}', end: '{}', type: {}]",
                    System.identityHashCode(this), watchId, start, end, type);
          registered.add(watchId);
          builder.setWatchId(watchId);
          builder.setType(Msg.WatchResponse.Type.CREATE);
          break;
        case CANCEL:
          watchId = req.getCancel().getWatchId();
          removeWatch(watchId);
          builder.setWatchId(watchId);
          builder.setType(Msg.WatchResponse.Type.CANCEL);
          break;
      }
      resp.onNext(builder.build());
    }

    @Override
    public void onError(Throwable t) {
      LOG.debug("Watch stream canceled [stream: {}]",
                System.identityHashCode(this));
      removeAllWatches();
    }

    @Override
    public void onCompleted() {
      LOG.debug("Watch stream completed [stream: {}]",
                System.identityHashCode(this));
      removeAllWatches();
      resp.onCompleted();
    }
  }

  private final class Watcher {
    private StreamObserver<Msg.WatchResponse> resp;
    private WatchRequestStream req;
    private boolean put;
    private boolean delete;

    Watcher(StreamObserver<Msg.WatchResponse> resp,
            WatchRequestStream req,
            Msg.WatchCreateRequest.Type type) {
      this.resp = resp;
      this.req = req;
      switch (type) {
        case PUT:
          put = true;
          delete = false;
          break;
        case DELETE:
          put = false;
          delete = true;
          break;
        case ALL:
          put = true;
          delete = true;
          break;
      }
    }

    public boolean isPutType() { return put; }
    public boolean isDeleteType() { return delete; }

    public void sendMsg(int watchId, Msg.WatchResponse msg) {
      if (req.isActive()) {
        try {
          resp.onNext(msg);
        } catch (StatusRuntimeException exc) {
          if (exc.getStatus().getCode() != Status.Code.CANCELLED) {
            LOG.warn("Watcher {} failed to send, got status {}",
                     watchId, exc.getStatus());
          }
          req.removeAllWatches();
        }
      }
    }
  }

  final class ServiceTracker {
    private String name;
    private Model.Service service;
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

    public synchronized boolean addOwnedKey(int instance, String key) {
      Model.Container container = getContainer(instance);
      assert container != null;  // pre-checked before calling
      if (!container.completed()) {
        container.addOwnedKey(key);
        return true;
      }
      return false;
    }

    public synchronized void removeOwnedKey(int instance, String key) {
      Model.Container container = getContainer(instance);
      assert container != null;  // should never get here.
      container.removeOwnedKey(key);
    }

    public void initialize() throws IOException {
      LOG.info("Initializing service '{}'.", name);
      // Request initial containers
      for (int i = 0; i < service.getInstances(); i++) {
        addContainer();
      }
    }

    private synchronized Model.Container getContainer(int instance) {
      if (instance >= 0 && instance < containers.size()) {
        return containers.get(instance);
      }
      return null;
    }

    public synchronized WebUI.ServiceContext toServiceContext() {
      WebUI.ServiceContext context = new WebUI.ServiceContext();
      context.name = name;
      context.numPending = waiting.size() + requested.size();
      context.numRunning = running.size();
      context.numSucceeded = numSucceeded;
      context.numKilled = numKilled;
      context.numFailed = numFailed;
      context.pending = Lists.newArrayListWithCapacity(context.numPending);
      context.running = Lists.newArrayListWithCapacity(context.numRunning);
      context.completed = Lists.newArrayListWithCapacity(
          context.numSucceeded + context.numKilled + context.numFailed);
      for (Model.Container container : containers) {
        WebUI.ContainerInfo info =
            new WebUI.ContainerInfo(container.getInstance(),
                                    container.getStartTime(),
                                    container.getFinishTime(),
                                    container.getState(),
                                    container.getLogsAddress());
        switch (info.state) {
          case WAITING:
          case REQUESTED:
            context.pending.add(info);
            break;
          case RUNNING:
            context.running.add(info);
            break;
          default:
            context.completed.add(info);
        }
      }
      return context;
    }

    public List<Model.Container> scale(int instances) {
      List<Model.Container> out =  new ArrayList<Model.Container>();

      // Any function that may remove containers needs to lock the kv store
      // outside the tracker to prevent deadlocks.
      synchronized (keyValueStore) {
        synchronized (this) {
          int active = getNumActive();
          int delta = instances - active;
          LOG.info("Scaling service '{}' to {} instances, a delta of {}.",
                   name, instances, delta);
          if (delta > 0) {
            // Scale up
            for (int i = 0; i < delta; i++) {
              out.add(addContainer());
              numTarget += 1;
            }
          } else if (delta < 0) {
            // Scale down
            for (int i = delta; i < 0; i++) {
              int instance;
              if (waiting.size() > 0) {
                instance = Utils.popfirst(waiting);
              } else if (requested.size() > 0) {
                instance = requested.get(requested.firstKey()).getInstance();
              } else {
                instance = Utils.popfirst(running);
              }
              finishContainer(instance, Model.Container.State.KILLED,
                              "Killed by user request.");
              out.add(containers.get(instance));
            }
          }
        }
      }
      return out;
    }

    private synchronized void requestContainer(Model.Container container) {
      Priority priority = newPriority(this);
      String[] nodes = (service.getNodes().isEmpty() ? null
                        : service.getNodes().toArray(new String[0]));
      String[] racks = (service.getRacks().isEmpty() ? null
                        : service.getRacks().toArray(new String[0]));
      boolean relaxLocality = ((nodes == null && racks == null) ? true
                               : service.getRelaxLocality());
      ContainerRequest req = new ContainerRequest(
          service.getResources(),
          nodes,
          racks,
          priority,
          relaxLocality,
          Strings.emptyToNull(service.getNodeLabel()));
      container.setContainerRequest(req);
      rmClient.addContainerRequest(req);
      requested.put(priority, container);
      LOG.info("REQUESTED: {}", container.getId());
    }

    public synchronized Model.Container addContainer() {
      Model.Container container;
      if (!isReady()) {
        container = new Model.Container(name, containers.size(),
                                        Model.Container.State.WAITING);
        waiting.add(container.getInstance());
        LOG.info("WAITING: {}", container.getId());
      } else {
        container = new Model.Container(name, containers.size(),
                                        Model.Container.State.REQUESTED);
        requestContainer(container);
      }
      containers.add(container);
      return container;
    }

    public Model.Container startContainer(final Container container,
        Resource resource) {
      LOG.info("Starting {}...", container.getId());
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
        out.setYarnNodeHttpAddress(container.getNodeHttpAddress());
        out.setResources(resource);

        ApplicationMaster.this.containers.put(container.getId(), out);
        running.add(instance);

        // Update container environment variables
        Map<String, String> env = new HashMap<String, String>(service.getEnv());
        updateServiceEnvironment(env, resource, out.getId());
        if (!ugi.isSecurityEnabled()) {
          // Add HADOOP_USER_NAME to environment for *simple* authentication only
          env.put("HADOOP_USER_NAME", ugi.getUserName());
        }

        final ContainerLaunchContext ctx =
            ContainerLaunchContext.newInstance(
                service.getLocalResources(),
                env,
                Arrays.asList(service.getScript()),
                null,
                tokens,
                spec.getAcls().getYarnAcls());

        totalMemory.addAndGet(resource.getMemory());
        totalVcores.addAndGet(resource.getVirtualCores());

        executor.execute(
            new Runnable() {
              @Override
              public void run() {
                try {
                  nmClient.startContainer(container, ctx);
                } catch (Throwable exc) {
                  LOG.warn("Failed to start {}_{}", ServiceTracker.this.name, instance, exc);
                  ServiceTracker.this.finishContainer(instance,
                      Model.Container.State.FAILED,
                      "Failed to start, exception raised: " + exc.getMessage());
                }
              }
            });

        LOG.info("RUNNING: {} on {}", out.getId(), container.getId());
      }

      if (!initialRunning && requested.size() == 0) {
        initialRunning = true;
        for (ServiceTracker dep : dependents) {
          dep.notifyRunning(name);
        }
      }
      return out;
    }

    public void finishContainer(int instance, Model.Container.State state, String exitMessage) {
      // Any function that may remove containers, needs to lock the kv store
      // outside the tracker to prevent deadlocks.
      synchronized (keyValueStore) {
        synchronized (this) {
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
              Resource resource = container.getResources();
              totalMemory.getAndAdd(-resource.getMemory());
              totalVcores.getAndAdd(-resource.getVirtualCores());
              break;
            default:
              return;  // Already finished, should never get here
          }

          boolean mayRestart = false;
          boolean warn = false;
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
              warn = true;
              break;
            default:
              throw new IllegalArgumentException(
                  "finishContainer got illegal state " + state);
          }

          if (warn) {
            LOG.warn("{}: {} - {}", state, container.getId(), exitMessage);
          } else {
            LOG.info("{}: {} - {}", state, container.getId(), exitMessage);
          }

          container.setState(state);
          container.setExitMessage(exitMessage);

          // Remove any owned keys from the key-value store
          for (String key : container.getOwnedKeys()) {
            Msg.KeyValue.Builder prevKv = keyValueStore.remove(key);
            // if not removed already, notify watchers
            if (prevKv != null) {
              // Message a single delete event with only the key set
              Msg.WatchResponse.Builder wrBuilder =
                  Msg.WatchResponse
                     .newBuilder()
                     .setType(Msg.WatchResponse.Type.DELETE)
                     .addEvent(prevKv.clearValue().clearOwner());
              for (IntervalTree.Item<Watcher> item : intervalTree.query(key)) {
                if (item.getValue().isDeleteType()) {
                  int watchId = item.getId();
                  item.getValue()
                      .sendMsg(watchId, wrBuilder.setWatchId(watchId).build());
                }
              }
            } else {
              LOG.error("Key '{}' already deleted, but wasn't removed from "
                        + "owned-keys set of service '{}'", key, name);
            }
          }
          container.clearOwnedKeys();

          if (mayRestart && (service.getMaxRestarts() == -1
              || numRestarted < service.getMaxRestarts())) {
            numRestarted += 1;
            LOG.info("RESTARTING: adding new container to replace {}.",
                     container.getId());
            addContainer();
          }

          if (isFinished()) {
            maybeShutdown();
          }
        }
      }
    }
  }

  class AppMasterImpl extends AppMasterGrpc.AppMasterImplBase {

    private boolean checkService(String name, StreamObserver<?> resp) {
      if (services.get(name) == null) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Unknown service '" + name + "'")
            .asRuntimeException());
        return false;
      }
      return true;
    }

    private boolean checkContainerInstance(String service, int instance,
                                           boolean checkNotCompleted,
                                           StreamObserver<?> resp) {
      if (!checkService(service, resp)) {
        return false;
      }
      ServiceTracker tracker = services.get(service);
      Model.Container container = tracker.getContainer(instance);
      if (container == null) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Service '" + service + "' has no container "
                             + "instance " + instance)
            .asRuntimeException());
        return false;
      }
      if (checkNotCompleted && container.completed()) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("Container '" + service + "_" + instance
                             + "' has already completed")
            .asRuntimeException());
        return false;
      }
      return true;
    }

    @Override
    public void shutdown(Msg.ShutdownRequest req,
        StreamObserver<Msg.Empty> resp) {
      FinalApplicationStatus status = MsgUtils.readFinalStatus(req.getFinalStatus());

      String message = req.getDiagnostics();
      if (message.isEmpty()) {
        message = "Shutdown requested by user.";
      }

      // Shutdown everything but the grpc server
      preshutdown(status, message);

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
      if (!checkContainerInstance(service, instance, false, resp)) {
        // invalid container id
        return;
      }

      services.get(service).finishContainer(instance, Model.Container.State.KILLED,
                                            "Killed by user request.");
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
      List<Model.Container> containers = tracker.scale(instances);
      // Lock on tracker to prevent containers from updating while writing. If
      // this proves costly, may want to copy beforehand.
      synchronized (tracker) {
        for (Model.Container c : containers) {
          msg.addContainers(MsgUtils.writeContainer(c));
        }
      }
      resp.onNext(msg.build());
      resp.onCompleted();
    }

    private SortedMap<String, Msg.KeyValue.Builder> selectRange(
          SortedMap<String, Msg.KeyValue.Builder> map,
          String start, String end,
          boolean openStart, boolean openEnd) {
      if (openStart && openEnd) {
        return map;
      } else if (openEnd) {
        return map.tailMap(start);
      } else if (openStart) {
        return map.headMap(end);
      } else if (start.compareTo(end) <= 0) {
        return map.subMap(start, end);
      }
      return null;
    }

    private Msg.GetRangeResponse.Builder evalGetRange(Msg.GetRangeRequest req) {
      String start = req.getStart();
      String end = req.getEnd();

      Msg.GetRangeResponse.Builder builder;

      synchronized (keyValueStore) {
        SortedMap<String, Msg.KeyValue.Builder> selection =
            selectRange(keyValueStore, start, end,
                        start.isEmpty() || start.equals("\u0000"),
                        end.isEmpty());

        builder = Msg.GetRangeResponse
                     .newBuilder()
                     .setCount(selection == null ? 0 : selection.size())
                     .setResultType(req.getResultType());

        if (selection != null) {
          switch (req.getResultType()) {
            case ITEMS:
              for (Map.Entry<String, Msg.KeyValue.Builder> entry : selection.entrySet()) {
                builder.addResult(entry.getValue());
              }
              break;
            case KEYS:
              for (String key : selection.keySet()) {
                builder.addResult(Msg.KeyValue.newBuilder().setKey(key));
              }
              break;
            case NONE:
              break;
          }
        }
      }
      return builder;
    }

    @Override
    public void setProgress(Msg.SetProgressRequest req,
        StreamObserver<Msg.Empty> resp) {
      float prog = req.getProgress();
      if (prog < 0.0f || prog > 1.0f) {
        resp.onError(Status.INVALID_ARGUMENT
            .withDescription("progress must be between 0 and 1, got " + prog)
            .asRuntimeException());
      } else {
        progress.set(prog);
        resp.onNext(MsgUtils.EMPTY);
        resp.onCompleted();
      }
    }

    @Override
    public void getRange(Msg.GetRangeRequest req,
        StreamObserver<Msg.GetRangeResponse> resp) {
      resp.onNext(evalGetRange(req).build());
      resp.onCompleted();
    }

    private Msg.DeleteRangeResponse.Builder evalDeleteRange(
        Msg.DeleteRangeRequest req) {
      String start = req.getStart();
      String end = req.getEnd();

      Msg.DeleteRangeResponse.Builder builder;

      synchronized (keyValueStore) {
        SortedMap<String, Msg.KeyValue.Builder> selection =
            selectRange(keyValueStore, start, end,
                        start.isEmpty() || start.equals("\u0000"),
                        end.isEmpty());

        builder = Msg.DeleteRangeResponse
                     .newBuilder()
                     .setCount(selection == null ? 0 : selection.size())
                     .setResultType(req.getResultType());

        if (selection != null && selection.size() > 0) {
          switch (req.getResultType()) {
            case ITEMS:
              for (Map.Entry<String, Msg.KeyValue.Builder> entry : selection.entrySet()) {
                builder.addResult(entry.getValue());
              }
              break;
            case KEYS:
              for (String key : selection.keySet()) {
                builder.addResult(Msg.KeyValue.newBuilder().setKey(key));
              }
              break;
            case NONE:
              break;
          }

          // Notify watchers, if any
          String firstKey = selection.firstKey();
          String lastKey = selection.lastKey();
          for (IntervalTree.Item<Watcher> item : intervalTree.query(firstKey, lastKey)) {
            if (item.getValue().isDeleteType()) {
              int watchId = item.getId();
              Msg.WatchResponse.Builder wrBuilder =
                  Msg.WatchResponse
                     .newBuilder()
                     .setWatchId(watchId)
                     .setType(Msg.WatchResponse.Type.DELETE);
              // Subselect the deleted keys based on the overlapping interval.
              // We need to floor/ceil the bounds since `subMap` rejects keys
              // out of the already subselected range
              String iStart = item.getIntervalBegin();
              String iEnd = item.getIntervalEnd();
              SortedMap<String, Msg.KeyValue.Builder> iSelection =
                  selectRange(selection, iStart, iEnd,
                              iStart.compareTo(firstKey) <= 0,
                              iEnd == null || iEnd.compareTo(lastKey) >= 0);

              for (String key : iSelection.keySet()) {
                wrBuilder.addEvent(Msg.KeyValue.newBuilder().setKey(key));
              }
              item.getValue().sendMsg(watchId, wrBuilder.build());
            }
          }

          // Do deletion
          // Clear owners first before deleting
          for (Map.Entry<String, Msg.KeyValue.Builder> entry : selection.entrySet()) {
            Msg.KeyValue.Builder value = entry.getValue();
            if (value.hasOwner()) {
              services.get(value.getOwner().getServiceName())
                      .removeOwnedKey(value.getOwner().getInstance(),
                                      entry.getKey());
            }
          }
          selection.clear();
        }
      }
      return builder;
    }

    @Override
    public void deleteRange(Msg.DeleteRangeRequest req,
        StreamObserver<Msg.DeleteRangeResponse> resp) {
      resp.onNext(evalDeleteRange(req).build());
      resp.onCompleted();
    }

    private boolean precheckPutKey(Msg.PutKeyRequest req, StreamObserver<?> resp) {
      synchronized (keyValueStore) {
        boolean ignoreValue = req.getIgnoreValue();
        boolean ignoreOwner = req.getIgnoreOwner();

        if (ignoreValue && ignoreOwner) {
          // can't ignore both value and owner
          resp.onError(Status.INVALID_ARGUMENT
              .withDescription("ignore_value & ignore_owner can't both be true")
              .asRuntimeException());
          return false;
        }

        if (ignoreValue && keyValueStore.get(req.getKey()) == null) {
          // ignore_value & key doesn't exist
          resp.onError(Status.FAILED_PRECONDITION
              .withDescription("ignore_value=True & key isn't already set")
              .asRuntimeException());
          return false;
        }

        Msg.ContainerInstance owner = req.hasOwner() ? req.getOwner() : null;

        if (!ignoreOwner && owner != null) {
          if (!checkContainerInstance(owner.getServiceName(),
                                      owner.getInstance(),
                                      true, resp)) {
            // Either invalid container id, or container already completed
            return false;
          }
        }
      }
      return true;
    }

    private Msg.PutKeyResponse.Builder evalPutKey(Msg.PutKeyRequest req) {
      String key = req.getKey();
      boolean ignoreValue = req.getIgnoreValue();
      boolean ignoreOwner = req.getIgnoreOwner();
      boolean returnPrevious = req.getReturnPrevious();

      Msg.KeyValue.Builder prev;

      synchronized (keyValueStore) {
        prev = keyValueStore.get(key);
        Msg.ContainerInstance owner = req.hasOwner() ? req.getOwner() : null;

        Msg.KeyValue.Builder kvBuilder = Msg.KeyValue.newBuilder().setKey(key);

        if (ignoreValue) {
          // prev == null was forbidden in precheckPutKey
          kvBuilder.setValue(prev.getValue());
        } else {
          kvBuilder.setValue(req.getValue());
        }

        if (ignoreOwner) {
          // Copy over previous owner if one exists
          if (prev != null && prev.hasOwner()) {
            kvBuilder.setOwner(prev.getOwner());
          }
        } else {
          // First clear any previous owner.
          if (prev != null && prev.hasOwner()) {
            services.get(prev.getOwner().getServiceName())
                    .removeOwnedKey(prev.getOwner().getInstance(), key);
          }
          // Only need to update internal state if we're setting a new owner
          if (owner != null) {
            boolean ok = services.get(owner.getServiceName())
                                 .addOwnedKey(owner.getInstance(), key);
            assert ok;  // fail if owner -> completed without locking kv store
            kvBuilder.setOwner(owner);
          }
        }
        keyValueStore.put(key, kvBuilder);

        // Notify watchers
        Msg.WatchResponse.Builder wrBuilder =
            Msg.WatchResponse
               .newBuilder()
               .setType(Msg.WatchResponse.Type.PUT)
               .addEvent(kvBuilder);

        for (IntervalTree.Item<Watcher> item : intervalTree.query(key)) {
          if (item.getValue().isPutType()) {
            int watchId = item.getId();
            item.getValue().sendMsg(watchId, wrBuilder.setWatchId(watchId).build());
          }
        }
      }

      Msg.PutKeyResponse.Builder builder =
          Msg.PutKeyResponse.newBuilder().setReturnPrevious(returnPrevious);

      if (returnPrevious && prev != null) {
        builder.setPrevious(prev);
      }

      return builder;
    }

    @Override
    public void putKey(Msg.PutKeyRequest req, StreamObserver<Msg.PutKeyResponse> resp) {
      synchronized (keyValueStore) {
        if (!precheckPutKey(req, resp)) {
          return;
        }
        resp.onNext(evalPutKey(req).build());
        resp.onCompleted();
      }
    }

    private int compareOwner(Msg.ContainerInstance lhs,
                             Msg.ContainerInstance rhs) {
      int out = lhs.getServiceName().compareTo(rhs.getServiceName());
      return out != 0 ? out : Integer.compare(lhs.getInstance(), rhs.getInstance());
    }

    private int compareValue(ByteString lhs, ByteString rhs) {
      return lhs.asReadOnlyByteBuffer().compareTo(rhs.asReadOnlyByteBuffer());
    }

    private boolean evalCondition(Msg.Condition cond) {
      synchronized (keyValueStore) {
        Msg.KeyValue.Builder kv = keyValueStore.get(cond.getKey());

        ByteString rhsValue = null;
        Msg.ContainerInstance rhsOwner = null;

        ByteString lhsValue = null;
        Msg.ContainerInstance lhsOwner = null;
        if (kv != null) {
          lhsValue = kv.getValue();
          if (kv.hasOwner()) {
            lhsOwner = kv.getOwner();
          }
        }

        Msg.Condition.Operator op = cond.getOperator();

        switch (cond.getRhsCase()) {
          case VALUE:
            rhsValue = cond.getValue();
            break;
          case OWNER:
            rhsOwner = cond.getOwner();
            break;
          case RHS_NOT_SET:
            break;
        }

        int compare = 0;

        switch (cond.getField()) {
          case VALUE:
            if (lhsValue == null || rhsValue == null) {
              // only check equality if null, all other comparisons are false
              switch (op) {
                case EQUAL:
                  return lhsValue == rhsValue;
                case NOT_EQUAL:
                  return lhsValue != rhsValue;
                default:
                  return false;
              }
            }
            compare = compareValue(lhsValue, rhsValue);
            break;
          case OWNER:
            if (lhsOwner == null || rhsOwner == null) {
              // only check equality if null, all other comparisons are false
              switch (op) {
                case EQUAL:
                  return lhsOwner == rhsOwner;
                case NOT_EQUAL:
                  return lhsOwner != rhsOwner;
                default:
                  return false;
              }
            }
            compare = compareOwner(lhsOwner, rhsOwner);
            break;
        }

        switch (op) {
          case EQUAL:
            return compare == 0;
          case NOT_EQUAL:
            return compare != 0;
          case LESS:
            return compare < 0;
          case LESS_EQUAL:
            return compare <= 0;
          case GREATER:
            return compare > 0;
          case GREATER_EQUAL:
            return compare >= 0;
        }
        return true;  // appease compiler, all cases are covered above
      }
    }

    @Override
    public void transaction(Msg.TransactionRequest req,
        StreamObserver<Msg.TransactionResponse> resp) {

      Msg.TransactionResponse.Builder builder =
          Msg.TransactionResponse.newBuilder();

      synchronized (keyValueStore) {
        // Evaluate all conditions
        boolean succeeded = true;
        for (Msg.Condition cond : req.getConditionList()) {
          if (!evalCondition(cond)) {
            succeeded = false;
            break;
          }
        }

        List<Msg.OpRequest> ops = succeeded ? req.getOnSuccessList() : req.getOnFailureList();

        // Validate all operations before committing any of them
        for (Msg.OpRequest op : ops) {
          switch (op.getRequestCase()) {
            case PUT_KEY:
              if (!precheckPutKey(op.getPutKey(), resp)) {
                return;
              }
              break;
            default:
              break;
          }
        }

        // Evaluate operations and build response list
        for (Msg.OpRequest op : ops) {
          switch (op.getRequestCase()) {
            case PUT_KEY:
              builder.addResult(Msg.OpResponse.newBuilder()
                     .setPutKey(evalPutKey(op.getPutKey())));
              break;
            case GET_RANGE:
              builder.addResult(Msg.OpResponse.newBuilder()
                     .setGetRange(evalGetRange(op.getGetRange())));
              break;
            case DELETE_RANGE:
              builder.addResult(Msg.OpResponse.newBuilder()
                     .setDeleteRange(evalDeleteRange(op.getDeleteRange())));
              break;
            default:
              break;
          }
        }
        builder.setSucceeded(succeeded);
      }

      resp.onNext(builder.build());
      resp.onCompleted();
    }

    @Override
    public StreamObserver<Msg.WatchRequest> watch(final StreamObserver<Msg.WatchResponse> resp) {
      return new WatchRequestStream(resp);
    }

    @Override
    public void addProxy(Msg.Proxy req, StreamObserver<Msg.Empty> resp) {
      ui.addProxy(req, resp);
    }

    @Override
    public void removeProxy(Msg.RemoveProxyRequest req, StreamObserver<Msg.Empty> resp) {
      ui.removeProxy(req, resp);
    }

    @Override
    public void uiInfo(Msg.UIInfoRequest req, StreamObserver<Msg.UIInfoResponse> resp) {
      ui.uiInfo(req, resp);
    }

    @Override
    public void getProxies(Msg.GetProxiesRequest req,
                           StreamObserver<Msg.GetProxiesResponse> resp) {
      ui.getProxies(req, resp);
    }
  }
}
