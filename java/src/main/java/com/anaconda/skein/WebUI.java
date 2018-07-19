package com.anaconda.skein;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.anaconda.skein.ApplicationMaster.ServiceTracker;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.ajax.JSON;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class WebUI {
  public static WebUI start(ApplicationId appId, Map<String, ServiceTracker> services)
      throws Exception {
    Server server = new Server(8080);
    server.setHandler(new HandlerList(
        new ResourceHandler() {
          {
            setResourceBase(WebUI.class.getResource("/META-INF/resources").toExternalForm());
          }
        },
        new WebApiHandler(appId, services),
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
    private ApplicationId appId;
    private Map<String, ServiceTracker> services;

    public WebApiHandler(ApplicationId appId, Map<String, ServiceTracker> services) {
      this.appId = appId;
      this.services = services;
    }

    public void handle(String target, Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
        throws IOException {
      if (baseRequest.isHandled() || !HttpMethod.GET.is(request.getMethod())) {
        return;
      }

      if (request.getPathInfo().equals("/api/application")) {
        baseRequest.setHandled(true);
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println(JSON.toString(appId));
      } else if (request.getPathInfo().equals("/api/containers")) {
        baseRequest.setHandled(true);
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(JSON.toString(
            Maps.transformValues(services, new Function<ServiceTracker, Object>() {
              public Object apply(ServiceTracker service) {
                return Lists.transform(service.getContainers(),
                    new Function<Model.Container, Object>() {
                      public Object apply(Model.Container container) {
                        return ImmutableMap.of(
                            "id", container.getId(),
                            "yarnContainerId", container.getYarnContainerId(),
                            "yarnNodeHttpAddress", container.getYarnNodeHttpAddress(),
                            "state", container.getState());
                      }
                    });
              }
            })));
      }
    }
  }
}
