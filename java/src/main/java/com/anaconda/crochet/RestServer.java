package com.anaconda.crochet;

import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;


public class RestServer {
    private static final EnumSet<DispatcherType> REQUEST_SCOPE = EnumSet.of(DispatcherType.REQUEST);

    private static final ConcurrentHashMap<String, byte[]> configuration
        = new ConcurrentHashMap<String,byte[]>();

    public static Integer port = -1;

    public static void main(String[] args) throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        String secret = System.getenv("CROCHET_SECRET_ACCESS_KEY");
        if (secret == null) {
            System.err.println("Couldn't find secret token at " +
                               "'CROCHET_SECRET_ACCESS_KEY' envar");
            System.exit(1);
        }

        context.setContextPath("/");
        FilterHolder holder = context.addFilter(AuthenticationFilter.class,
                                                "/*", REQUEST_SCOPE);
        holder.setInitParameter("secret", secret);

        // Add the key-value store servlet
        context.addServlet(new ServletHolder(new KeyValueServlet(configuration)), "/keys/*");

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
