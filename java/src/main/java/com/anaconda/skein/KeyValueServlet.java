package com.anaconda.skein;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class KeyValueServlet extends HttpServlet {
  private static ConcurrentHashMap<String, byte[]> keystore;

  public KeyValueServlet(ConcurrentHashMap<String, byte[]> keystore) {
    this.keystore = keystore;
  }

  private String getKey(HttpServletRequest req) {
    String key = req.getPathInfo();
    // Strips leading `/` from keys, and replaces empty keys with null
    // Ensures that /keys and /keys/ are treated the same
    return (key == null || key.length() <= 1) ? null : key.substring(1);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    String key = getKey(req);

    if (key == null) {
      // Handle /keys or /keys/
      // Returns an object like {'keys': [key1, key2, ...]}
      ObjectMapper mapper = new ObjectMapper();
      ArrayNode arrayNode = mapper.createArrayNode();
      ObjectNode objectNode = mapper.createObjectNode();
      for (String key2 : keystore.keySet()) {
        arrayNode.add(key2);
      }
      objectNode.putPOJO("keys", arrayNode);

      OutputStream out = resp.getOutputStream();
      mapper.writeValue(out, objectNode);
      out.close();
      return;
    }

    byte[] value = keystore.get(key);
    if (value == null) {
      Utils.sendError(resp, 404, "Missing key");
      return;
    }

    OutputStream out = resp.getOutputStream();
    out.write(value);
    out.close();
  }

  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    String key = getKey(req);
    byte[] value = IOUtils.toByteArray(req.getInputStream());

    if (key == null || value.length == 0) {
      Utils.sendError(resp, 400, "Malformed Request");
      return;
    }

    keystore.put(key, value);
    resp.setStatus(204);
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    String key = getKey(req);

    if (key == null) {
      Utils.sendError(resp, 400, "Malformed Request");
      return;
    }

    if (keystore.remove(key) == null) {
      Utils.sendError(resp, 404, "Missing key");
      return;
    }

    resp.setStatus(204);
  }
}
