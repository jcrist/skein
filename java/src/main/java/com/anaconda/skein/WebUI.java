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
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
                        Map<String, ServiceContext> services)
      throws Exception {

    server = new Server(port);
    server.setHandler(new HandlerList(
        new WebApiHandler(appId, keyValueStore, services),
        new ResourceHandler() {
          {
            setDirectoriesListed(false);
            setResourceBase(WebUI.class.getResource("/META-INF/resources").toExternalForm());
          }
        },
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
    Map<String, ServiceContext> services = new HashMap<String, ServiceContext>();
    ServiceContext service1 = new ServiceContext();
    service1.name = "Service 1";
    service1.running = 3;
    service1.succeeded = 1;
    service1.stopped = 1;
    service1.failed = 1;
    service1.active = Lists.newArrayList(
        new ContainerInfo(1, 0, 1, Model.Container.State.WAITING, ""),
        new ContainerInfo(2, 0, 1, Model.Container.State.REQUESTED, ""),
        new ContainerInfo(4, 0, 1, Model.Container.State.RUNNING, "https://dummyurl.html")
    );
    service1.completed = Lists.newArrayList(
        new ContainerInfo(0, 0, 1, Model.Container.State.SUCCEEDED, "https://dummyurl.html"),
        new ContainerInfo(3, 0, 1, Model.Container.State.KILLED, "https://dummyurl.html"),
        new ContainerInfo(5, 0, 1, Model.Container.State.FAILED, "https://dummyurl.html")
    );
    services.put("Service 1", service1);
    ServiceContext service2 = new ServiceContext();
    service2.name = "Service 2";
    service2.running = 1;
    service2.succeeded = 0;
    service2.stopped = 0;
    service2.failed = 0;
    service2.active = Lists.newArrayList(
        new ContainerInfo(0, 0, 1, Model.Container.State.RUNNING, "https://dummyurl.html")
    );
    services.put("Service 2", service2);

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
  }

  public static class ServiceContext {
    public String name;
    public int running;
    public int succeeded;
    public int stopped;
    public int failed;
    public List<ContainerInfo> active;
    public List<ContainerInfo> completed;

    public ServiceContext() {}
  }

  private static class Context {
    private final String appId;
    private final Map<String, ServiceContext> services;
    private final Map<String, Msg.KeyValue.Builder> keyValueStore;

    public Context(String appId,
                   Map<String, Msg.KeyValue.Builder> keyValueStore,
                   Map<String, ServiceContext> services) {
      this.appId = appId;
      this.keyValueStore = keyValueStore;
      this.services = services;
    }

    public String appId() { return appId; }

    public DecoratedCollection<Map.Entry<String, String>> kv() {
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
        return new DecoratedCollection<Map.Entry<String, String>>(out);
      }
    }

    public DecoratedCollection<Map.Entry<String, ServiceContext>> services() {
      List<Map.Entry<String, ServiceContext>> entries =
          Lists.newArrayList(services.entrySet());
      Collections.sort(entries, new Comparator<Map.Entry<String, ServiceContext>>() {
        public int compare(Map.Entry<String, ServiceContext> e1,
                            Map.Entry<String, ServiceContext> e2) {
          return e1.getKey().compareTo(e2.getKey());
        }
      });
      return new DecoratedCollection<Map.Entry<String, ServiceContext>>(entries);
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
                         Map<String, ServiceContext> services) {
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
