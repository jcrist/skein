package com.anaconda.skein;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import java.io.IOException;
import java.io.OutputStream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ClientServlet extends HttpServlet {
  private final Client client;
  private final ObjectMapper mapper;

  public ClientServlet(Client client) {
    this.client = client;
    this.mapper = new ObjectMapper();
  }

  private ApplicationId appIdFromString(String appId) {
    // Parse applicationId_{timestamp}_{id}
    String[] parts = appId.split("_");
    if (parts.length < 3) {
      return null;
    }
    long timestamp = Long.valueOf(parts[1]);
    int id = Integer.valueOf(parts[2]);
    return ApplicationId.newInstance(timestamp, id);
  }

  private ApplicationId getAppId(HttpServletRequest req) {
    String appId = req.getPathInfo();
    if (appId == null || appId.length() <= 1) {
      return null;
    }
    return appIdFromString(appId.substring(1));
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    ApplicationId appId = getAppId(req);

    if (appId == null) {
      Utils.sendError(resp, 404, "Malformed request");
      return;
    }

    ApplicationReport report = client.getApplicationReport(appId);

    if (report == null) {
      Utils.sendError(resp, 404, "Unknown ApplicationID");
      return;
    }

    ObjectNode objectNode = mapper.createObjectNode();
    objectNode.put("id", appId.toString());
    objectNode.put("state", report.getYarnApplicationState().toString());
    objectNode.put("finalStatus", report.getFinalApplicationStatus().toString());
    objectNode.put("user", report.getUser());
    objectNode.put("trackingURL", report.getTrackingUrl());
    objectNode.put("host", report.getHost());
    objectNode.put("rpcPort", report.getRpcPort());

    OutputStream out = resp.getOutputStream();
    mapper.writeValue(out, objectNode);
    out.close();
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    ApplicationId appId = getAppId(req);

    if (appId != null) {
      Utils.sendError(resp, 400, "Malformed Request");
      return;
    }

    Msg.Job job;
    try {
      job = mapper.readValue(req.getInputStream(), Msg.Job.class);
      job.validate();
    } catch (IOException exc) {
      Utils.sendError(resp, 400, exc.getMessage());
      return;
    } catch (IllegalArgumentException exc) {
      Utils.sendError(resp, 400, exc.getMessage());
      return;
    }

    try {
      appId = client.submit(job);
    } catch (Exception exc) {
      Utils.sendError(resp, 400, "Failed to launch application");
      return;
    }
    OutputStream out = resp.getOutputStream();
    out.write(appId.toString().getBytes());
    resp.setStatus(200);
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    ApplicationId appId = getAppId(req);

    if (appId == null) {
      Utils.sendError(resp, 400, "Malformed Request");
      return;
    }

    if (client.killApplication(appId)) {
      resp.setStatus(204);
    } else {
      Utils.sendError(resp, 404, "Failed to kill application");
    }
  }
}
