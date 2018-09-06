package com.anaconda.skein;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.util.DecoratedCollection;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.rewrite.handler.RedirectPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class WebUI {
  private static final Logger LOG = LogManager.getLogger(WebUI.class);

  private Server server;

  public WebUI() {}

  public void configure(int port,
                        String appId,
                        Map<String, Msg.KeyValue.Builder> keyValueStore,
                        List<ServiceContext> services)
      throws Exception {

    server = new Server(port);

    // Handler for all static resources
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    // Hack to get directory containing resources, since `URI.resolve` doesn't
    // work on opaque (e.g. non-filesystem) paths.
    URI baseURI = URI.create(
        WebUI.class.getResource("/META-INF/resources/favicon.ico").toURI()
                   .toASCIIString().replaceFirst("/favicon.ico$","/")
    );
    LOG.info("Serving Resources From: " + baseURI);
    context.setBaseResource(Resource.newResource(baseURI));
    context.addServlet(new ServletHolder("default", DefaultServlet.class), "/");

    // Issue a 302 redirect to services from homepage
    RewriteHandler rewrite = new RewriteHandler();
    rewrite.addRule(new RedirectPatternRule("", "/services"));

    server.setHandler(new HandlerList(
        rewrite,
        new WebApiHandler(appId, keyValueStore, services),
        context,
        new DefaultHandler()));
  }

  public void start() throws Exception {
    server.start();
  }

  public void stop() throws Exception {
    server.stop();
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

    WebUI webui = new WebUI();
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
        new ContainerInfo(4, 0, 3 * 60 + 24, Model.Container.State.RUNNING, url)
    );
    service1.completed = Lists.newArrayList(
        new ContainerInfo(0, 0, 30, Model.Container.State.SUCCEEDED, url),
        new ContainerInfo(3, 0, 90, Model.Container.State.KILLED, url),
        new ContainerInfo(5, 0, 60 * 60 * 2 + 90, Model.Container.State.FAILED, url)
    );
    services.add(service1);
    ServiceContext service2 = new ServiceContext();
    service2.name = "Service 2";
    service2.running = 1;
    service2.succeeded = 0;
    service2.killed = 0;
    service2.failed = 0;
    service2.active = Lists.newArrayList(
        new ContainerInfo(0, 0, 24, Model.Container.State.RUNNING, url)
    );
    service2.completed = Lists.newArrayList();
    services.add(service2);

    try {
      webui.configure(port, "application_1526497750451_0001", kv, services);
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
    public Model.Container.State status;
    public String logAddress;

    public ContainerInfo(int instance, long startTime, long finishTime,
                         Model.Container.State status, String logAddress) {
      this.instance = instance;
      this.startTime = startTime;
      this.finishTime = finishTime;
      this.status = status;
      this.logAddress = logAddress;
    }

    public String runtime() {
      long delta;
      switch (status) {
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
      secs = finishTime - startTime;
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

  private static class Context {
    private final String appId;
    private final List<ServiceContext> services;
    private final Map<String, Msg.KeyValue.Builder> keyValueStore;

    public Context(String appId,
                   Map<String, Msg.KeyValue.Builder> keyValueStore,
                   List<ServiceContext> services) {
      this.appId = appId;
      this.keyValueStore = keyValueStore;
      this.services = services;
    }

    public String appId() { return appId; }

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
      List<ServiceContext> entries = Lists.newArrayList(services);
      Collections.sort(entries, new Comparator<ServiceContext>() {
        public int compare(ServiceContext e1, ServiceContext e2) {
          return e1.name.compareTo(e2.name);
        }
      });
      return new DecoratedCollection<ServiceContext>(entries);
    }
  }

  private static class WebApiHandler extends AbstractHandler {
    private final Mustache servicesTemplate = new DefaultMustacheFactory()
        .compile("services.mustache.html");

    private final Mustache kvTemplate = new DefaultMustacheFactory()
        .compile("kv.mustache.html");

    private final Context context;

    public WebApiHandler(String appId,
                         Map<String, Msg.KeyValue.Builder> keyValueStore,
                         List<ServiceContext> services) {
      this.context = new Context(appId, keyValueStore, services);
    }

    public void handle(String target, Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
        throws IOException {

      if (baseRequest.isHandled() || !HttpMethod.GET.is(request.getMethod())) {
        return;
      }

      if (request.getPathInfo().equals("/services")) {
        baseRequest.setHandled(true);
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        servicesTemplate.execute(response.getWriter(), context);
      } else if (request.getPathInfo().equals("/kv")) {
        baseRequest.setHandled(true);
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        kvTemplate.execute(response.getWriter(), context);
      }
    }
  }
}
