package com.anaconda.crochet;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.DispatcherType;

public class ApplicationMaster {
  private static final Logger LOG = LogManager.getLogger(ApplicationMaster.class);

  private static final EnumSet<DispatcherType> REQUEST_SCOPE =
      EnumSet.of(DispatcherType.REQUEST);

  private static String secret;

  private static final ConcurrentHashMap<String, byte[]> configuration =
      new ConcurrentHashMap<String, byte[]>();

  private static Integer privatePort = -1;

  private static Integer publicPort = -1;

  private static Server server;

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
    privateConnector.setPort(8080);
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
  }

  /** Initialize the ApplicationMaster. **/
  public void init() {
    secret = System.getenv("CROCHET_SECRET_ACCESS_KEY");

    if (secret == null) {
      LOG.fatal("Couldn't find secret token at "
                + "'CROCHET_SECRET_ACCESS_KEY' envar");
      System.exit(1);
    }
  }

  /** Run the ApplicationMaster. **/
  public void run() throws Exception {
    startupRestServer();
    server.join();
  }

  /** Main entrypoint for the ApplicationMaster. **/
  public static void main(String[] args) throws Exception {
    ApplicationMaster appMaster = new ApplicationMaster();

    appMaster.init();
    appMaster.run();
  }
}
