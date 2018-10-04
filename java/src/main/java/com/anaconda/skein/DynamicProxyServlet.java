package com.anaconda.skein;

import org.eclipse.jetty.proxy.ProxyServlet;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class DynamicProxyServlet extends ProxyServlet {
  private final Map<String, String> mapping;
  private final Lock lock;

  public DynamicProxyServlet(Map<String, String> mapping, Lock lock) {
    this.mapping = mapping;
    this.lock = lock;
  }

  protected void onProxyRewriteFailed(HttpServletRequest req, HttpServletResponse resp) {
    resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
    try {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    } catch (IOException e) {
      // Ignore, just means the jetty error page may not be displayed. The 404
      // is still sent
    }
  }

  private String getPrefix(String path) {
    int index = path.indexOf("/", 1);
    return (index == -1) ? path.substring(1, path.length()) : path.substring(1, index);
  }

  public String rewriteTarget(HttpServletRequest request) {
    String path = request.getPathInfo();

    // No path to dispatch on
    if (path == null) {
      return null;
    }

    String prefix = getPrefix(path);
    String target;

    lock.lock();
    try {
      target = mapping.get(prefix);
    } finally {
      lock.unlock();
    }

    if (target == null) {
      return null;
    }

    StringBuilder uri = new StringBuilder(target);

    String rest = path.substring(1 + prefix.length());
    if (!rest.isEmpty()) {
      if (!rest.startsWith("/")) {
        uri.append("/");
      }
      uri.append(rest);
    }

    String query = request.getQueryString();
    if (query != null) {
      // Is there at least one path segment ?
      String separator = "://";
      if (uri.indexOf("/", uri.indexOf(separator) + separator.length()) < 0) {
        uri.append("/");
      }
      uri.append("?").append(query);
    }
    URI rewrittenURI = URI.create(uri.toString()).normalize();

    if (!validateDestination(rewrittenURI.getHost(), rewrittenURI.getPort())) {
      return null;
    }

    String out = rewrittenURI.toString();
    return out;
  }
}
