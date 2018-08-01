package com.anaconda.skein;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class WebUI {
  public static WebUI start(ApplicationId appId,
                            Map<String, Msg.KeyValue> keyValueStore,
                            Map<String, List<Model.Container>> services)
      throws Exception {
    Server server = new Server(0);
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
      private final Map<String, Msg.KeyValue> keyValueStore;
      private final Map<String, List<Model.Container>> services;

      public Context(ApplicationId appId,
                     Map<String, Msg.KeyValue> keyValueStore,
                     Map<String, List<Model.Container>> services) {
        this.appId = appId;
        this.keyValueStore = keyValueStore;
        this.services = services;
      }

      public Iterable<Map.Entry<String, String>> kv() {
        return Maps.transformValues(keyValueStore, new Function<Msg.KeyValue, String>() {
          public String apply(Msg.KeyValue kv) {
            return kv.getValue().toString(StandardCharsets.US_ASCII);
          }
        }).entrySet();
      }

      public String appId() { return appId.toString(); }

      public List<Map.Entry<String, List<Model.Container>>> services() {
        List<Map.Entry<String, List<Model.Container>>> entries = Lists.newArrayList(
            services.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, List<Model.Container>>>() {
          public int compare(Map.Entry<String, List<Model.Container>> e1,
                             Map.Entry<String, List<Model.Container>> e2) {
            return e1.getKey().compareTo(e2.getKey());
          }
        });
        return entries;
      }
    }

    private final Mustache indexMustache = new DefaultMustacheFactory()
        .compile("index.mustache.html");
    private final Context context;

    public WebApiHandler(ApplicationId appId,
                         Map<String, Msg.KeyValue> keyValueStore,
                         Map<String, List<Model.Container>> services) {
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
}
