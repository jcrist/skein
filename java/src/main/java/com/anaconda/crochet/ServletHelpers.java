package com.anaconda.crochet;

import java.io.IOException;
import javax.servlet.http.HttpServletResponse;

public class ServletHelpers {
  /** Return a formatted error response. **/
  public static void sendError(HttpServletResponse resp, int code, String msg)
      throws IOException {
    resp.resetBuffer();
    resp.setStatus(code);
    resp.setHeader("Content-Type", "application/json");
    resp.getOutputStream().print("{\"errorMessage\":\"" + msg + "\"}");
    resp.flushBuffer();
  }

  public static void sendError(HttpServletResponse resp, int code)
      throws IOException {
    sendError(resp, code, "unknown error");
  }
}
