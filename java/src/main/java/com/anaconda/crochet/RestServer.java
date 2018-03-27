package com.anaconda.crochet;

import java.util.EnumSet;
import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;


public class RestServer {
    private static final EnumSet<DispatcherType> REQUEST_SCOPE = EnumSet.of(DispatcherType.REQUEST);

    public static Integer port = -1;

    public static void main(String[] args) throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);

        context.setContextPath("/");
        context.addFilter(AuthenticationFilter.class, "/*", REQUEST_SCOPE);

        // Add the key-value store servlet
        context.addServlet(new ServletHolder(new KeyValueServlet()), "/keys/*");

        // Start the server
        Server server = new Server(8080);
        server.setHandler(context);
        server.start();

        // Determine port used
        port = ((ServerConnector)server.getConnectors()[0]).getLocalPort();
        System.out.println("Listening at localhost:" + port);

        // Run until closing
        server.join();
    }
}
