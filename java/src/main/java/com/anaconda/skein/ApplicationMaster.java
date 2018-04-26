package com.anaconda.skein;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  private final Map<String, List<Model.Service>> waiting =
      new HashMap<String, List<Model.Service>>();
  private final List<RSPair> resourceToService = new ArrayList<RSPair>();

  private Integer privatePort;
  private Integer publicPort;
  private String hostname;

  private int numTotal = 1;
  private AtomicInteger numCompleted = new AtomicInteger();
  private AtomicInteger numFailed = new AtomicInteger();
  private AtomicInteger numAllocated = new AtomicInteger();

  private final ConcurrentHashMap<ContainerId, Container> containers =
      new ConcurrentHashMap<ContainerId, Container>();

  private AMRMClientAsync rmClient;
  private NMClientAsync nmClient;
  private UserGroupInformation ugi;
  private ByteBuffer tokens;

  private void loadJob() throws Exception {
    try {
      job = Utils.MAPPER.readValue(new File(".skein.json"), Model.Job.class);
    } catch (IOException exc) {
      fatal("Issue loading job specification", exc);
    }
    job.validate(true);

    for (Map.Entry<String, Model.Service> entry : job.getServices().entrySet()) {
      Model.Service service = entry.getValue();

      resourceToService.add(new RSPair(service.getResources(), service));

      if (!service.initialize(entry.getKey(), secret)) {
        for (String key : service.getDepends()) {
          List<Model.Service> lk = waiting.get(key);
          if (lk == null) {
            lk = new ArrayList<Model.Service>();
            waiting.put(key, lk);
          }
          lk.add(service);
        }
      }
    }

    // Sort resource lookup, so that resource matching works later
    Collections.sort(resourceToService);
  }

  private void startupRestServer() throws Exception {
    // Configure the server
    server = new Server();
    HandlerCollection handlers = new HandlerCollection();
    server.setHandler(handlers);
    server.setStopAtShutdown(true);

    // Create the servlets once
    final ServletHolder keyVal = new ServletHolder(new KeyValueServlet());

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
    commands.add("sleep 10 "
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

  @SuppressWarnings("unchecked")
  private void initializeServices() {
    for (int i = 0; i < numTotal; i++) {
      Priority priority = Priority.newInstance(0);
      Resource resource = Resource.newInstance(10, 1);
      rmClient.addContainerRequest(new ContainerRequest(resource,
                                                        null,
                                                        null,
                                                        priority));
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

  public void run() throws Exception {
    conf = new YarnConfiguration();

    secret = System.getenv("SKEIN_SECRET_ACCESS_KEY");
    if (secret == null) {
      fatal("Couldn't find secret token at 'SKEIN_SECRET_ACCESS_KEY' envar");
    }

    loadJob();

    try {
      hostname = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException exc) {
      fatal("Couldn't determine hostname for appmaster", exc);
    }

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

    startupRestServer();

    rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
    rmClient.init(conf);
    rmClient.start();

    nmClient = NMClientAsync.createNMClientAsync(this);
    nmClient.init(conf);
    nmClient.start();

    rmClient.registerApplicationMaster(hostname, privatePort, "");

    initializeServices();

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

  private class KeyValueServlet extends HttpServlet {
    private String getKey(HttpServletRequest req) {
      String key = req.getPathInfo();
      // Strips leading `/` from keys, and replaces empty keys with null
      // Ensures that /keys and /keys/ are treated the same
      return (key == null || key.length() <= 1) ? null : key.substring(1);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {

      String key = getKey(req);

      if (key == null) {
        // Handle /keys or /keys/
        // Returns an object like {'keys': [key1, key2, ...]}
        ArrayNode arrayNode = Utils.MAPPER.createArrayNode();
        ObjectNode objectNode = Utils.MAPPER.createObjectNode();
        for (String key2 : configuration.keySet()) {
          arrayNode.add(key2);
        }
        objectNode.putPOJO("keys", arrayNode);

        resp.setHeader("Content-Type", "application/json");
        OutputStream out = resp.getOutputStream();
        Utils.MAPPER.writeValue(out, objectNode);
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

      String key = getKey(req);
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
      if (waiting.containsKey(key)) {
        for (Model.Service s: waiting.remove(key)) {
          if (s.notifySet()) {
            s.prepare(secret, configuration);
          }
        }
      }

      resp.setStatus(204);
    }
  }

  /* Utilitiy inner classes */

  public static class RSPair implements Comparable<RSPair> {
    public final Resource resource;
    public final Model.Service service;

    public RSPair(Resource r, Model.Service s) {
      resource = r;
      service = s;
    }

    public int compareTo(RSPair other) {
      return resource.compareTo(other.resource);
    }
  }
}
