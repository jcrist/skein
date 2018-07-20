package com.anaconda.skein;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.anaconda.skein.ApplicationMaster.ServiceTracker;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class WebUI {
  public static WebUI start(ApplicationId appId,
                            Map<String, String> keyValueStore,
                            Map<String, ServiceTracker> services)
      throws Exception {
    Server server = new Server(8080);
    server.setHandler(new HandlerList(
        new WebApiHandler(appId, keyValueStore, services),
        new ResourceHandler() {
          {
            setDirectoriesListed(false);
            setResourceBase(WebUI.class.getResource("/META-INF/resources").toExternalForm());
          }
        },
        new DefaultHandler()));
    server.start();
    return new WebUI(server);
  }

  private final Server server;

  private WebUI(Server server) {
    this.server = server;
  }

  public void stop() throws Exception {
    server.stop();
  }

  public URI getURI() {
    return server.getURI();
  }

  private static class WebApiHandler extends AbstractHandler {
    private static class Context {
      private final ApplicationId appId;
      private final Map<String, String> keyValueStore;
      private final Map<String, ServiceTracker> services;

      public Context(ApplicationId appId,
                     Map<String, String> keyValueStore,
                     Map<String, ServiceTracker> services) {
        this.appId = appId;
        this.keyValueStore = keyValueStore;
        this.services = services;
      }

      public Iterable<Map.Entry<String, String>> kv() { return keyValueStore.entrySet(); }

      public String appId() { return appId.toString(); }

      public Iterable<Map.Entry<String, Iterable<Model.Container>>> services() {
        return Maps.transformValues(
            services,
            new Function<ServiceTracker, Iterable<Model.Container>>() {
              public Iterable<Model.Container> apply(ServiceTracker serviceTracker) {
                return serviceTracker.getContainers();
              }
            }).entrySet();
      }
    }

    private final Mustache indexMustache = new DefaultMustacheFactory()
        .compile("index.mustache.html");
    private final Context context;

    public WebApiHandler(ApplicationId appId,
                         Map<String, String> keyValueStore,
                         Map<String, ServiceTracker> services) {
      this.context = new Context(appId, keyValueStore, services);
    }

    public void handle(String target, Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
        throws IOException {
      if (baseRequest.isHandled() || !HttpMethod.GET.is(request.getMethod())) {
        return;
      }

      if (request.getPathInfo().equals("/")) {
        baseRequest.setHandled(true);
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        indexMustache.execute(response.getWriter(), context);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    WebUI.start(
        ApplicationId.newInstance(42, 42),
        ImmutableMap.of("foo", "bar"),
        Collections.<String, ServiceTracker>emptyMap());
  }
}
