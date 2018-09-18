package com.anaconda.skein;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.util.DecoratedCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jetty.rewrite.handler.RedirectPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.servlet.DispatcherType;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class WebUI {
  private static final Logger LOG = LogManager.getLogger(WebUI.class);

  private Server server;

  private WebUI(Server server) {
    this.server = server;
  }

  public static WebUI create(int port,
                             String appId,
                             Map<String, Msg.KeyValue.Builder> keyValueStore,
                             List<ServiceContext> services,
                             Configuration conf,
                             boolean testing)
      throws Exception {

    // Set the jetty log level
    System.setProperty("org.eclipse.jetty.LEVEL", "WARN");

    Server server = new Server(port);

    // Handler for all static resources
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    // Hack to get directory containing resources, since `URI.resolve` doesn't
    // work on opaque (e.g. non-filesystem) paths.
    URI baseURI = URI.create(
        WebUI.class.getResource("/META-INF/resources/favicon.ico").toURI()
                   .toASCIIString().replaceFirst("/favicon.ico$", "/")
    );
    LOG.info("Serving resources from: " + baseURI);
    context.setBaseResource(Resource.newResource(baseURI));
    context.addServlet(new ServletHolder("default", DefaultServlet.class), "/");

    final String prefix = WebAppUtils.getHttpSchemePrefix(conf);
    UIModel uiModel = new UIModel(appId, keyValueStore, services, prefix);
    context.addServlet(
        new ServletHolder(new TemplateServlet(uiModel, "services.mustache.html")),
        "/services");
    context.addServlet(
        new ServletHolder(new TemplateServlet(uiModel, "kv.mustache.html")),
        "/kv");

    // Add the yarn proxy filter
    if (!testing) {
      FilterHolder filter = context.addFilter(
          AmIpFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
      List<String> proxies = WebAppUtils.getProxyHostsAndPortsForAmFilter(conf);
      final String proxyBase = System.getenv(
          ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV);
      String hosts = Joiner.on(AmIpFilter.PROXY_HOSTS_DELIMITER).join(
          Lists.transform(proxies,
            new Function<String, String>() {
              public String apply(String proxy) {
                return proxy.split(":", 2)[0];
              }
            })
      );
      String uriBases = Joiner.on(AmIpFilter.PROXY_URI_BASES_DELIMITER).join(
          Lists.transform(proxies,
            new Function<String, String>() {
              public String apply(String proxy) {
                return prefix + proxy + proxyBase;
              }
            })
      );
      filter.setInitParameter(AmIpFilter.PROXY_HOSTS, hosts);
      filter.setInitParameter(AmIpFilter.PROXY_URI_BASES, uriBases);
    }

    // Issue a 302 redirect to services from homepage
    RewriteHandler rewrite = new RewriteHandler();
    RedirectPatternRule redirect = new RedirectPatternRule();
    redirect.setPattern("");
    redirect.setLocation("/services");
    rewrite.addRule(redirect);
    context.insertHandler(rewrite);

    server.setHandler(context);

    return new WebUI(server);
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

  public static void main(String[] args) {
    if (args.length != 1) {
      LOG.fatal("Usage: <command> port");
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
    ServiceContext service1 = new ServiceContext();
    service1.name = "Service 1";
    service1.running = 3;
    service1.succeeded = 1;
    service1.killed = 1;
    service1.failed = 1;
    service1.active = Lists.newArrayList(
        new ContainerInfo(1, 0, 0, Model.Container.State.WAITING, ""),
        new ContainerInfo(2, 0, 0, Model.Container.State.REQUESTED, ""),
        new ContainerInfo(4, System.currentTimeMillis() - (3 * 60 + 24) * 1000, 0,
                          Model.Container.State.RUNNING, url)
    );
    service1.completed = Lists.newArrayList(
        new ContainerInfo(0, 0, 30 * 1000, Model.Container.State.SUCCEEDED, url),
        new ContainerInfo(3, 0, 90 * 1000, Model.Container.State.KILLED, url),
        new ContainerInfo(5, 0, (60 * 60 * 2 + 90) * 1000,
                          Model.Container.State.FAILED, url)
    );
    services.add(service1);
    ServiceContext service2 = new ServiceContext();
    service2.name = "Service 2";
    service2.running = 1;
    service2.succeeded = 0;
    service2.killed = 0;
    service2.failed = 0;
    service2.active = Lists.newArrayList(
        new ContainerInfo(0, (System.currentTimeMillis() - (24 * 1000)), 0,
                          Model.Container.State.RUNNING, url)
    );
    service2.completed = Lists.newArrayList();
    services.add(service2);

    try {
      WebUI webui = WebUI.create(port, "application_1526497750451_0001",
                                 kv, services, new YarnConfiguration(), true);
      webui.start();
    } catch (Throwable exc) {
      LOG.fatal("Error running WebUI", exc);
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
      long secs = delta / 1000;
      long hours = secs / (60 * 60);
      secs = secs % (60 * 60);
      long mins = secs / 60;
      secs = secs % 60;

      if (hours > 0) {
        return String.format("%dh %dm", hours, mins);
      }
      else if (mins > 0) {
        return String.format("%dm %ds", mins, secs);
      } else {
        return String.format("%ds", secs);
      }
    }
  }

  public static class ServiceContext {
    public String name;
    public int running;
    public int succeeded;
    public int killed;
    public int failed;
    public List<ContainerInfo> active;
    public List<ContainerInfo> completed;

    public ServiceContext() {}
  }

  private static class UIModel {
    public final String appId;
    private final List<ServiceContext> services;
    private final Map<String, Msg.KeyValue.Builder> keyValueStore;
    public final String prefix;

    public UIModel(String appId,
                   Map<String, Msg.KeyValue.Builder> keyValueStore,
                   List<ServiceContext> services,
                   String prefix) {
      this.appId = appId;
      this.keyValueStore = keyValueStore;
      this.services = services;
      this.prefix = prefix;
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
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_OK);
      template.execute(response.getWriter(), uiModel);
    }
  }
}
