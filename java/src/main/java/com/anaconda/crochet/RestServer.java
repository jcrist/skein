package com.anaconda.crochet;

import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class RestServer {
  private static final EnumSet<DispatcherType> REQUEST_SCOPE =
      EnumSet.of(DispatcherType.REQUEST);

  private static String secret;

  private static final ConcurrentHashMap<String, byte[]> configuration =
      new ConcurrentHashMap<String, byte[]>();

  private static Integer privatePort = -1;

  private static Integer publicPort = -1;

  public static void main(String[] args) throws Exception {
    secret = System.getenv("CROCHET_SECRET_ACCESS_KEY");

    if (secret == null) {
      System.err.println("Couldn't find secret token at "
                         + "'CROCHET_SECRET_ACCESS_KEY' envar");
      System.exit(1);
    }

    // Configure the server
    Server server = new Server();
    HandlerCollection handlers = new HandlerCollection();
    server.setHandler(handlers);

    // Create the servlets once
    ServletHolder keyVal =
        new ServletHolder(new KeyValueServlet(configuration));

    // This connector serves content authenticated by the secret key
    ServerConnector privateConnector = new ServerConnector(server);
    privateConnector.setPort(8080);
    privateConnector.setName("Private");
    server.addConnector(privateConnector);

    ServletContextHandler privateContext =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    privateContext.setContextPath("/");
    privateContext.setVirtualHosts(new String[] {"@Private"});
    privateContext.addServlet(keyVal, "/keys/*");
    FilterHolder holder =
        privateContext.addFilter(HMACFilter.class, "/*", REQUEST_SCOPE);
    holder.setInitParameter("secret", secret);
    handlers.addHandler(privateContext);

    // This connector serves content unauthenticated
    ServerConnector publicConnector = new ServerConnector(server);
    publicConnector.setPort(8081);
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
    System.out.println("Private access at localhost:" + privatePort);
    System.out.println("Public access at localhost:" + publicPort);

    // Run until closing
    server.join();
  }
}
