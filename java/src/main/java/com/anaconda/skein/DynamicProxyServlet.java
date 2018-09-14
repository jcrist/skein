package com.anaconda.skein;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jetty.proxy.ProxyServlet;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import javax.servlet.http.HttpServletRequest;

public class DynamicProxyServlet extends ProxyServlet {
  private static final Logger LOG = LogManager.getLogger(WebUI.class);

  private final Map<String, String> mapping;
  private final Lock lock;

  public DynamicProxyServlet(Map<String, String> mapping, Lock lock) {
    this.mapping = mapping;
    this.lock = lock;
  }

  private String getPrefix(String path) {
    int index = path.indexOf("/", 1);
    return (index == -1) ? path.substring(1, path.length()) : path.substring(1, index);
  }

  public String rewriteTarget(HttpServletRequest request) {
    String path = request.getPathInfo();
    LOG.info("path: " + path);

    // No path to dispatch on
    if (path == null) {
      return null;
    }

    String prefix = getPrefix(path);
    String target;

    LOG.info("prefix: " + prefix);

    lock.lock();
    try {
      target = mapping.get(prefix);
    } finally {
      lock.unlock();
    }

    LOG.info("target: " + target);

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
    LOG.info("out: " + out);
    return out;
  }
}
