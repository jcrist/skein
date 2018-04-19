package com.anaconda.skein;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import javax.servlet.http.HttpServletResponse;

public class Utils {
  public static final ObjectMapper MAPPER = new SkeinObjectMapper();

  /** Return a formatted error response. **/
  public static void sendError(HttpServletResponse resp, int code, String msg)
      throws IOException {
    resp.resetBuffer();
    resp.setStatus(code);
    resp.setHeader("Content-Type", "application/json");

    ObjectNode node = MAPPER.createObjectNode();
    node.put("error", msg);
    MAPPER.writeValue(resp.getOutputStream(), node);

    resp.flushBuffer();
  }

  public static void sendError(HttpServletResponse resp, int code)
      throws IOException {
    sendError(resp, code, "unknown error");
  }

  /** Given a path as a String, convert it to a Path. Also converts
   * all local paths to absolute. **/
  public static Path normalizePath(String path) {
    URI uri;
    try {
      uri = new URI(path);
      if (uri.getScheme() == null) {
        uri = null;
      }
    } catch (URISyntaxException exc) {
      uri = null;
    }
    if (uri == null) {
      uri = new File(path).getAbsoluteFile().toURI();
    }
    return new Path(uri);
  }

  /** Compare two filesystems for equality.
   *
   * Borrowed (with some modification) from Apache Spark. License header:
   * --------------------------------------------------------------------
   *
   * Licensed to the Apache Software Foundation (ASF) under one or more
   * contributor license agreements.  See the NOTICE file distributed with
   * this work for additional information regarding copyright ownership.
   * The ASF licenses this file to You under the Apache License, Version 2.0
   * (the "License"); you may not use this file except in compliance with
   * the License.  You may obtain a copy of the License at
   *
   *    http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   **/
  public static boolean equalFs(FileSystem srcFs, FileSystem dstFs) {
    String srcScheme = srcFs.getScheme();
    String dstScheme = dstFs.getScheme();

    if (srcScheme == null || dstScheme == null || !srcScheme.equals(dstScheme)) {
      return false;
    }

    URI srcUri = srcFs.getUri();
    URI dstUri = dstFs.getUri();

    String srcAuth = srcUri.getAuthority();
    String dstAuth = dstUri.getAuthority();
    if (srcAuth != null && dstAuth != null && !srcAuth.equalsIgnoreCase(dstAuth)) {
      return false;
    }

    String srcHost = srcUri.getHost();
    String dstHost = dstUri.getHost();

    if (srcHost != null && dstHost != null && !srcHost.equals(dstHost)) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
      } catch (UnknownHostException exc) {
        return false;
      }
    }

    return srcHost.equals(dstHost) && srcUri.getPort() == dstUri.getPort();
  }
}
