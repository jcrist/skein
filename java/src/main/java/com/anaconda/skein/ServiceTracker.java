package com.anaconda.skein;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ServiceTracker {
  private String name;
  private Model.Service service;
  private int count = 0;
  private List<String> commands;
  private Map<String, String> env;
  private AtomicInteger numWaitingOn;
  private boolean ready;

  public ServiceTracker(String name, Model.Service service) {
    this.name = name;
    this.service = service;

    Set<String> depends = service.getDepends();
    if (depends != null && depends.size() > 0) {
      numWaitingOn = new AtomicInteger(depends.size());
      this.ready = false;
    }
    this.ready = true;
  }

  public boolean isReady() { return ready; }

  private String formatConfig(Map<String, String> config, String val) {
    if (config != null) {
      for (Map.Entry<String, String> item : config.entrySet()) {
        val = val.replace("%(" + item.getKey() + ")", item.getValue());
      }
    }
    return val;
  }

  public void prepare(String secret, Map<String, String> config) {
    commands = new ArrayList<String>();
    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    String pipeLogs = (" 1>>" + logdir + "/" + name + ".stdout "
                        + "2>>" + logdir + "/" + name + ".stderr;");
    for (String c : service.getCommands()) {
      commands.add(formatConfig(config, c) + pipeLogs);
    }

    env = new HashMap<String, String>();
    env.put("SKEIN_SECRET_ACCESS_KEY", secret);
    for (Map.Entry<String, String> item : service.getEnv().entrySet()) {
      env.put(item.getKey(), formatConfig(config, item.getValue()));
    }
  }

  public boolean notifySet() {
    return numWaitingOn.decrementAndGet() == 0;
  }

  public static enum ContainerState {
    WAITING,    // Waiting on service dependencies
    REQUESTED,  // Container requested, waiting to run
    RUNNING,    // Currently running
    FINISHED,   // Successfully finished running
    FAILED,     // Errored or was killed by yarn
    STOPPED     // Stopped by user
  }

  public static class Container {
    private Model.Service service;
    private ContainerState state;

    public Container(Model.Service service, ContainerState state) {
      this.service = service;
      this.state = state;
    }

    public ContainerState getState() { return state; }
    public void setState(ContainerState state) { this.state = state; }

    public String toString() {
      return "Container<state: " + state + ">";
    }
  }
}
