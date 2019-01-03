package com.anaconda.skein;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.protobuf.ByteString;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.util.DecoratedCollection;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.eclipse.jetty.rewrite.handler.RedirectPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Slf4jLog;
import org.eclipse.jetty.util.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class WebUI {
  private static final Logger LOG = LoggerFactory.getLogger(WebUI.class);

  private final Map<String, Msg.Proxy> routeToProxy = new HashMap<String, Msg.Proxy>();
  protected final Map<String, String> nameToRoute = new TreeMap<String, String>();
  private final Map<String, String> prefixToTarget = new HashMap<String, String>();
  private static final String PROXY_PREFIX = "/pages";
  private final List<String> uiAddresses;
  private final Server server;
  private final Lock writeLock;
  private final Lock readLock;

  public WebUI(int port,
               String appId,
               String appName,
               String user,
               String amLogsAddress,
               boolean hasDriver,
               AtomicDouble progress,
               AtomicDouble totalMemory,
               AtomicInteger totalVcores,
               long startTimeMillis,
               Map<String, Msg.KeyValue.Builder> keyValueStore,
               List<ServiceContext> services,
               Set<String> users,
               Configuration conf,
               boolean testing) throws Exception {

    // Create necessary locks
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    writeLock = rwLock.writeLock();
    readLock = rwLock.readLock();

    // Setup the jetty loggers
    Log.setLog(new Slf4jLog());

    // Create the server
    server = new Server(port);

    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");

    // Add a handler for all static resources
    // Hack to get directory containing resources, since `URI.resolve` doesn't
    // work on opaque (e.g. non-filesystem) paths.
    URI baseURI = URI.create(
        WebUI.class.getResource("/META-INF/resources/favicon.ico").toURI()
                   .toASCIIString().replaceFirst("/favicon.ico$", "/")
    );
    LOG.debug("Serving resources from {}", baseURI);
    context.setBaseResource(Resource.newResource(baseURI));
    context.addServlet(new ServletHolder("default", DefaultServlet.class), "/");

    // Add application servlets
    final String protocol = WebAppUtils.getHttpSchemePrefix(conf);
    UIModel uiModel = new UIModel(appId, appName, user, amLogsAddress, hasDriver,
                                  progress, totalMemory, totalVcores, startTimeMillis,
                                  keyValueStore, services, protocol);
    context.addServlet(
        new ServletHolder(new TemplateServlet(uiModel, "overview.mustache.html")),
        "/overview");
    context.addServlet(
        new ServletHolder(new TemplateServlet(uiModel, "kv.mustache.html")),
        "/kv");
    context.addServlet(
        new ServletHolder(new DynamicProxyServlet(prefixToTarget, readLock)),
        PROXY_PREFIX + "/*");

    // Add the yarn proxy filter
    if (!testing) {
      FilterHolder filter = context.addFilter(
          AmIpFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
      List<String> proxies = WebAppUtils.getProxyHostsAndPortsForAmFilter(conf);
      final String proxyBase = System.getenv(
          ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV);

      String proxyHosts = Joiner.on(AmIpFilter.PROXY_HOSTS_DELIMITER).join(
          Lists.transform(proxies,
            new Function<String, String>() {
              public String apply(String proxy) {
                return proxy.split(":", 2)[0];
              }
            }));

      List<String> proxyURIBases = Lists.newArrayList(
          Lists.transform(proxies,
            new Function<String, String>() {
              public String apply(String proxy) {
                return protocol + proxy + proxyBase;
              }
            }));
      filter.setInitParameter(AmIpFilter.PROXY_HOSTS, proxyHosts);
      filter.setInitParameter(AmIpFilter.PROXY_URI_BASES,
          Joiner.on(AmIpFilter.PROXY_URI_BASES_DELIMITER).join(proxyURIBases));

      if (users != null) {
        LOG.info("UI ACLs are enabled, restricted to {} users", users.size());
        context.addFilter(new FilterHolder(new AccessFilter(users)),
                          "/*", EnumSet.allOf(DispatcherType.class));
      }

      // The uiAddresses are the proxy bases with a trailing slash
      uiAddresses = Lists.newArrayList();
      for (String base : proxyURIBases) {
        uiAddresses.add(base + "/");
      }
    } else {
      uiAddresses = Lists.newArrayList();
    }

    // Issue a 302 redirect to overview from homepage
    RewriteHandler rewrite = new RewriteHandler();
    RedirectPatternRule redirect = new RedirectPatternRule();
    redirect.setPattern("");
    redirect.setLocation("/overview");
    rewrite.addRule(redirect);
    context.insertHandler(rewrite);

    server.setHandler(context);
  }

  public void start() throws Exception {
    server.start();
  }

  public void stop() {
    try {
      server.stop();
    } catch (Exception e) {
      LOG.error("Failed to stop UI server", e);
    }
  }

  public URI getURI() {
    return server.getURI();
  }

  public void addProxy(Msg.Proxy req, StreamObserver<Msg.Empty> resp) {
    String route = req.getRoute();
    String target = req.getTarget();
    String name = req.getLinkName();

    if (route.contains("/")) {
      resp.onError(Status.INVALID_ARGUMENT
          .withDescription("Page page routes must not contain '/'")
          .asRuntimeException());
      return;
    }

    if (target.endsWith("/")) {
      target.substring(0, target.length() - 1);
    }

    URL targetURL;
    try {
      targetURL = new URL(target);
    } catch (MalformedURLException exc) {
      resp.onError(Status.INVALID_ARGUMENT
          .withDescription("Page target address '" + target
                           + "' is an invalid URL:\n" + exc.getMessage())
          .asRuntimeException());
      return;
    }

    // If the target address has no path or is a directory, the link route
    // needs a trailing slash to resolve relative links correctly.
    String linkRoute = route;
    String targetPath = targetURL.getPath();
    if (targetPath.isEmpty() || targetPath.endsWith("/")) {
      linkRoute = route + "/";
    }

    writeLock.lock();
    try {
      if (routeToProxy.containsKey(route)) {
        resp.onError(Status.ALREADY_EXISTS
            .withDescription("A proxied page with route '" + route
                             + "' already exists")
            .asRuntimeException());
        return;
      }
      if (!name.isEmpty() && nameToRoute.containsKey(name)) {
        resp.onError(Status.ALREADY_EXISTS
            .withDescription("A proxied page with name '" + name
                             + "' already exists")
            .asRuntimeException());
        return;
      }
      routeToProxy.put(route, req);
      if (!name.isEmpty()) {
        nameToRoute.put(name, linkRoute);
      }
      prefixToTarget.put(route, target);
    } finally {
      writeLock.unlock();
    }

    LOG.info("Added proxied page [route: '{}', target: '{}', name: '{}']",
             route, target, name);

    resp.onNext(MsgUtils.EMPTY);
    resp.onCompleted();
  }

  public void removeProxy(Msg.RemoveProxyRequest req,
                          StreamObserver<Msg.Empty> resp) {
    String route = req.getRoute();

    writeLock.lock();
    try {
      Msg.Proxy prev = routeToProxy.remove(route);
      if (prev == null) {
        resp.onError(Status.FAILED_PRECONDITION
            .withDescription("A proxied page with route '" + route
                             + "' doesn't exist")
            .asRuntimeException());
        return;
      }
      prefixToTarget.remove(route);
      String name = prev.getLinkName();
      if (!name.isEmpty()) {
        nameToRoute.remove(name);
      }
    } finally {
      writeLock.unlock();
    }

    LOG.info("Removed proxied page [route: '{}']", route);

    resp.onNext(MsgUtils.EMPTY);
    resp.onCompleted();
  }

  public void uiInfo(Msg.UIInfoRequest req,
                     StreamObserver<Msg.UIInfoResponse> resp) {
    Msg.UIInfoResponse.Builder msg =
        Msg.UIInfoResponse
           .newBuilder()
           .addAllUiAddress(uiAddresses)
           .setProxyPrefix(PROXY_PREFIX);

    resp.onNext(msg.build());
    resp.onCompleted();
  }

  public void getProxies(Msg.GetProxiesRequest req,
                         StreamObserver<Msg.GetProxiesResponse> resp) {

    Msg.GetProxiesResponse.Builder msg = Msg.GetProxiesResponse.newBuilder();

    writeLock.lock();
    try {
      msg.addAllProxy(routeToProxy.values());
    } finally {
      writeLock.unlock();
    }

    resp.onNext(msg.build());
    resp.onCompleted();
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      LOG.error("Usage: <command> port");
      System.exit(1);
    }
    int port = Integer.parseInt(args[0]);

    // Create a dummy key-value store
    Map<String, Msg.KeyValue.Builder> kv = new TreeMap<String, Msg.KeyValue.Builder>();
    kv.put("Key 1", Msg.KeyValue.newBuilder().setValue(ByteString.copyFromUtf8("Val 1")));
    kv.put("Key 2", Msg.KeyValue.newBuilder().setValue(ByteString.copyFromUtf8("Val 2")));
    kv.put("Key 3", Msg.KeyValue.newBuilder()
                       .setValue(ByteString.copyFrom(new byte[] {(byte) 255})));
    // Create a dummy services map
    String url = "https://dummyurl.html";
    List<ServiceContext> services = Lists.newArrayList();
    long now = System.currentTimeMillis();
    ServiceContext service1 = new ServiceContext();
    service1.name = "my-first-service";
    service1.numPending = 1;
    service1.pending = Lists.newArrayList(
        new ContainerInfo(5, 0, 0, Model.Container.State.REQUESTED, "")
    );
    service1.numRunning = 2;
    service1.running = Lists.newArrayList(
        new ContainerInfo(3, now - (3 * 60 + 24) * 1000, 0,
                          Model.Container.State.RUNNING, url),
        new ContainerInfo(4, now - (2 * 60 + 45) * 1000, 0,
                          Model.Container.State.RUNNING, url)
    );
    service1.numSucceeded = 1;
    service1.numKilled = 1;
    service1.numFailed = 1;
    service1.completed = Lists.newArrayList(
        new ContainerInfo(0, 0, (60 * 60 * 2 + 90) * 1000,
                          Model.Container.State.FAILED, url),
        new ContainerInfo(1, 0, 90 * 1000, Model.Container.State.KILLED, url),
        new ContainerInfo(2, 0, 30 * 1000, Model.Container.State.SUCCEEDED, url)
    );
    services.add(service1);
    ServiceContext service2 = new ServiceContext();
    service2.name = "my-second-service";
    service2.numPending = 0;
    service2.pending = Lists.newArrayList();
    service2.numRunning = 1;
    service2.running = Lists.newArrayList(
        new ContainerInfo(0, now - (24 * 1000), 0, Model.Container.State.RUNNING, url)
    );
    service2.numSucceeded = 0;
    service2.numKilled = 0;
    service2.numFailed = 0;
    service2.completed = Lists.newArrayList();
    services.add(service2);

    try {
      WebUI webui = new WebUI(port,
                              "application_1539033277709_0001",
                              "Demo Application",
                              "alice",
                              url,
                              false,
                              new AtomicDouble(0.35),
                              new AtomicDouble(2 * 2048 + 512 + 256),
                              new AtomicInteger(5),
                              now - (60 * 60 * 2 + 120) * 1000,
                              kv,
                              services,
                              null,
                              new YarnConfiguration(),
                              true);
      webui.nameToRoute.put("name1", "page1");
      webui.nameToRoute.put("name2", "page2");
      webui.start();
    } catch (Throwable exc) {
      LOG.error("Error running WebUI", exc);
      System.exit(1);
    }
  }

  public static class ContainerInfo {
    public int instance;
    public long startTime;
    public long finishTime;
    public Model.Container.State state;
    public String logsAddress;

    public ContainerInfo(int instance, long startTime, long finishTime,
                         Model.Container.State state, String logsAddress) {
      this.instance = instance;
      this.startTime = startTime;
      this.finishTime = finishTime;
      this.state = state;
      this.logsAddress = logsAddress;
    }

    public String runtime() {
      long delta;
      switch (state) {
        case WAITING:
        case REQUESTED:
          return "0s";
        case RUNNING:
          delta = System.currentTimeMillis() - startTime;
          break;
        default:
          delta = finishTime - startTime;
      }
      return Utils.formatRuntime(delta);
    }
  }

  public static class ServiceContext {
    public String name;
    public int numPending;
    public int numRunning;
    public int numSucceeded;
    public int numKilled;
    public int numFailed;
    public List<ContainerInfo> pending;
    public List<ContainerInfo> running;
    public List<ContainerInfo> completed;

    public ServiceContext() {}
  }

  private class UIModel {
    public final String appId;
    public final String appName;
    public final String user;
    public final String amLogsAddress;
    public final boolean hasDriver;
    private final AtomicDouble progress;
    private final AtomicDouble totalMemory;
    private final AtomicInteger totalVcores;
    private final long startTimeMillis;
    private final List<ServiceContext> services;
    private final Map<String, Msg.KeyValue.Builder> keyValueStore;
    public final String protocol;

    public UIModel(String appId,
                   String appName,
                   String user,
                   String amLogsAddress,
                   boolean hasDriver,
                   AtomicDouble progress,
                   AtomicDouble totalMemory,
                   AtomicInteger totalVcores,
                   long startTimeMillis,
                   Map<String, Msg.KeyValue.Builder> keyValueStore,
                   List<ServiceContext> services,
                   String protocol) {
      this.appId = appId;
      this.appName = appName;
      this.user = user;
      this.amLogsAddress = amLogsAddress;
      this.hasDriver = hasDriver;
      this.progress = progress;
      this.totalMemory = totalMemory;
      this.totalVcores = totalVcores;
      this.startTimeMillis = startTimeMillis;
      this.keyValueStore = keyValueStore;
      this.services = services;
      this.protocol = protocol;
    }

    public String progress() {
      double val = progress.get();
      return (val < 0) ? "N/A" : String.format("%.1f%%", val * 100);
    }

    public String totalMemory() {
      return Utils.formatMemory(totalMemory.get());
    }

    public int totalVcores() {
      return totalVcores.get();
    }

    public String runtime() {
      return Utils.formatRuntime(System.currentTimeMillis() - startTimeMillis);
    }

    public String proxyPrefix() {
      return PROXY_PREFIX;
    }

    public List<Map.Entry<String, String>> kv() {
      synchronized (keyValueStore) {
        List<Map.Entry<String, String>> out =
            Lists.newArrayListWithCapacity(keyValueStore.size());
        for (Map.Entry<String, Msg.KeyValue.Builder> entry : keyValueStore.entrySet()) {
          ByteString value = entry.getValue().getValue();
          out.add(Maps.immutableEntry(entry.getKey(),
                                      value.isValidUtf8()
                                      ? value.toStringUtf8()
                                      : "<binary value>"));
        }
        return out;
      }
    }

    public List<Map.Entry<String, String>> pages() {
      readLock.lock();
      try {
        return Lists.newArrayList(nameToRoute.entrySet());
      } finally {
        readLock.unlock();
      }
    }

    public DecoratedCollection<ServiceContext> services() {
      return new DecoratedCollection<ServiceContext>(services);
    }
  }

  private static class TemplateServlet extends HttpServlet {
    private final Mustache template;
    private final UIModel uiModel;

    public TemplateServlet(UIModel uiModel, String templatePath) {
      this.uiModel = uiModel;
      this.template = new DefaultMustacheFactory().compile(templatePath);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_OK);
      template.execute(response.getWriter(), uiModel);
    }
  }

  private static class AccessFilter implements Filter {
    Set<String> users;

    public AccessFilter(Set<String> users) {
      this.users = users;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain chain) throws IOException, ServletException {
      HttpServletRequest req = (HttpServletRequest) request;
      HttpServletResponse resp = (HttpServletResponse) response;

      String user = req.getRemoteUser();
      if (!users.contains(user)) {
        resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
        resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        resp.sendError(HttpServletResponse.SC_FORBIDDEN,
                       "User is not authorized to access this page.");
        return;
      }
      chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }
  }
}
