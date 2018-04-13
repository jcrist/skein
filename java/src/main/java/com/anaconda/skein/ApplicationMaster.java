package com.anaconda.skein;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.DispatcherType;


public class ApplicationMaster implements AMRMClientAsync.CallbackHandler,
       NMClientAsync.CallbackHandler {

  private static final Logger LOG = LogManager.getLogger(ApplicationMaster.class);

  private static final EnumSet<DispatcherType> REQUEST_SCOPE =
      EnumSet.of(DispatcherType.REQUEST);

  private Configuration conf;

  private static String secret;

  private static Server server;

  private static final ConcurrentHashMap<String, byte[]> configuration =
      new ConcurrentHashMap<String, byte[]>();

  private static Integer privatePort = -1;
  private static Integer publicPort = -1;
  private String hostname = "";

  private int numTotal = 1;
  private AtomicInteger numCompleted = new AtomicInteger();
  private AtomicInteger numFailed = new AtomicInteger();
  private AtomicInteger numAllocated = new AtomicInteger();

  private ConcurrentHashMap<ContainerId, Container> containers =
      new ConcurrentHashMap<ContainerId, Container>();

  private AMRMClientAsync rmClient;
  private NMClientAsync nmClient;
  private UserGroupInformation ugi;
  private ByteBuffer tokens;

  private void startupRestServer() throws Exception {
    // Configure the server
    server = new Server();
    HandlerCollection handlers = new HandlerCollection();
    server.setHandler(handlers);
    server.setStopAtShutdown(true);

    // Create the servlets once
    final ServletHolder keyVal =
        new ServletHolder(new KeyValueServlet(configuration));

    // This connector serves content authenticated by the secret key
    ServerConnector privateConnector = new ServerConnector(server);
    privateConnector.setPort(0);
    privateConnector.setName("Private");
    server.addConnector(privateConnector);

    ServletContextHandler privateContext =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    privateContext.setContextPath("/");
    privateContext.setVirtualHosts(new String[] {"@Private"});
    privateContext.addServlet(keyVal, "/keys/*");
    FilterHolder holder =
        privateContext.addFilter(HmacFilter.class, "/*", REQUEST_SCOPE);
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
    publicContext.addServlet(keyVal, "/keys/*");
    handlers.addHandler(publicContext);

    // Startup the server
    server.start();

    // Determine ports
    privatePort = privateConnector.getLocalPort();
    publicPort = publicConnector.getLocalPort();
  }

  /* ResourceManager Callbacks */

  @Override
  public void onContainersCompleted(List<ContainerStatus> containerStatuses) {
    for (ContainerStatus status : containerStatuses) {

      int exitStatus = status.getExitStatus();

      if (exitStatus == 0) {
        numCompleted.incrementAndGet();
      } else if (exitStatus != ContainerExitStatus.ABORTED) {
        numCompleted.incrementAndGet();
        numFailed.incrementAndGet();
      } else {
        numAllocated.decrementAndGet();
      }
    }
    shutdown();
  }

  @Override
  public void onContainersAllocated(List<Container> newContainers) {

    numAllocated.addAndGet(newContainers.size());

    List<String> commands = new ArrayList<String>();
    commands.add("sleep 180 "
                 + "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                 + "/container.stdout "
                 + "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                 + "/container.stderr");

    ContainerLaunchContext ctx =
        ContainerLaunchContext.newInstance(null, null, commands, null,
                                           tokens, null);

    for (Container c : newContainers) {
      containers.put(c.getId(), c);
      nmClient.startContainerAsync(c, ctx);
    }
  }

  @Override
  public void onShutdownRequest() {
    shutdown();
  }

  @Override
  public void onNodesUpdated(List<NodeReport> nodeReports) {}

  @Override
  public float getProgress() {
    return (float)numCompleted.get() / numTotal;
  }

  @Override
  public void onError(Throwable exc) {
    shutdown();
  }

  /* NodeManager Callbacks */

  @Override
  public void onContainerStarted(ContainerId containerId,
                                 Map<String, ByteBuffer> allServiceResponse) {
    Container container = containers.get(containerId);
    if (container != null) {
      nmClient.getContainerStatusAsync(containerId, container.getNodeId());
    }
  }

  @Override
  public void onContainerStatusReceived(ContainerId containerId,
                                        ContainerStatus containerStatus) {}

  @Override
  public void onContainerStopped(ContainerId containerId) {
    containers.remove(containerId);
  }

  @Override
  public void onStartContainerError(ContainerId containerId, Throwable exc) {
    containers.remove(containerId);
    numCompleted.incrementAndGet();
    numFailed.incrementAndGet();
  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable exc) {}

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable exc) {
    containers.remove(containerId);
  }

  /** Initialize the ApplicationMaster. **/
  public void init() {
    conf = new YarnConfiguration();
    secret = System.getenv("SKEIN_SECRET_ACCESS_KEY");

    if (secret == null) {
      LOG.fatal("Couldn't find secret token at "
                + "'SKEIN_SECRET_ACCESS_KEY' envar");
      System.exit(1);
    }

    try {
      hostname = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException exc) {
      LOG.fatal("Couldn't determine hostname for appmaster");
      System.exit(1);
    }
  }

  /** Run the ApplicationMaster. **/
  @SuppressWarnings("unchecked")
  public void run() throws Exception {
    startupRestServer();

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

    // Create ugi and add original tokens to it
    String userName = System.getenv(Environment.USER.name());
    ugi = UserGroupInformation.createRemoteUser(userName);
    ugi.addCredentials(credentials);

    rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
    rmClient.init(conf);
    rmClient.start();

    nmClient = NMClientAsync.createNMClientAsync(this);
    nmClient.init(conf);
    nmClient.start();

    rmClient.registerApplicationMaster(hostname, privatePort, "");

    for (int i = 0; i < numTotal; i++) {
      Priority priority = Priority.newInstance(0);
      Resource resource = Resource.newInstance(10, 1);
      rmClient.addContainerRequest(new ContainerRequest(resource,
                                                        null,
                                                        null,
                                                        priority));
    }

    server.join();
  }

  private void shutdown() {
    // wait for completion.
    nmClient.stop();

    FinalApplicationStatus status;
    String msg = ("Diagnostics."
                  + ", total=" + numTotal
                  + ", completed=" + numCompleted.get()
                  + ", allocated=" + numAllocated.get()
                  + ", failed=" + numFailed.get());

    if (numFailed.get() == 0 && numCompleted.get() == numTotal) {
      status = FinalApplicationStatus.SUCCEEDED;
    } else {
      status = FinalApplicationStatus.FAILED;
    }

    try {
      rmClient.unregisterApplicationMaster(status, msg, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException ex) {
      LOG.error("Failed to unregister application", ex);
    }

    rmClient.stop();
    try {
      server.stop();
    } catch (Exception ex) {
      LOG.error("Failed to properly shutdown the jetty server");
      LOG.error(ex);
      System.exit(1);
    }
  }

  /** Main entrypoint for the ApplicationMaster. **/
  public static void main(String[] args) {
    ApplicationMaster appMaster = new ApplicationMaster();

    appMaster.init();
    try {
      appMaster.run();
    } catch (Throwable exc) {
      LOG.fatal("Error running ApplicationMaster", exc);
      System.exit(1);
    }
  }
}
