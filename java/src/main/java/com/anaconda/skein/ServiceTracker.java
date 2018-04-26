package com.anaconda.skein;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class ServiceTracker implements Comparable<ServiceTracker> {
  private String name;
  private Model.Service service;
  private List<String> commands;
  private Map<String, String> env;
  private AtomicInteger numWaitingOn;
  private boolean ready;
  ContainerLaunchContext ctx;

  // Application State
  private final Map<UUID, ContainerRecord> waiting =
      new HashMap<UUID, ContainerRecord>();
  private final Map<UUID, ContainerRecord> requested =
      new HashMap<UUID, ContainerRecord>();
  private final Map<UUID, ContainerRecord> running =
      new HashMap<UUID, ContainerRecord>();
  private final Map<UUID, ContainerRecord> finished =
      new HashMap<UUID, ContainerRecord>();
  private final Map<UUID, ContainerRecord> failed =
      new HashMap<UUID, ContainerRecord>();
  private final Map<UUID, ContainerRecord> stopped =
      new HashMap<UUID, ContainerRecord>();

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

  public int compareTo(ServiceTracker other) {
    return service.getResources().compareTo(other.service.getResources());
  }

  public boolean matches(Resource r) {
    // requested and requirement <= response
    return requested.size() > 0 && service.getResources().compareTo(r) <= 0;
  }

  public void launchContainer(Container c, NMClientAsync nmClient) {
    ContainerRecord container = null;
    UUID uuid = null;
    for (UUID key : requested.keySet()) {
      uuid = key;
      container = requested.remove(uuid);
      break;
    }
    assert container != null;
    assert uuid != null;
    container.setContainerId(c.getId());
    container.setState(ContainerState.RUNNING);
    running.put(uuid, container);
    nmClient.startContainerAsync(c, ctx);
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

  public void initialize(String secret, Map<String, String> config,
                         ByteBuffer tokens, AMRMClientAsync rmClient) {
    commands = new ArrayList<String>();
    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    String pipeLogs = (" 1>>" + logdir + "/" + name + ".stdout "
                        + "2>>" + logdir + "/" + name + ".stderr;");
    for (String c : service.getCommands()) {
      commands.add(formatConfig(config, c) + pipeLogs);
    }

    env = new HashMap<String, String>();
    env.put("SKEIN_SECRET_ACCESS_KEY", secret);
    if (service.getEnv() != null) {
      for (Map.Entry<String, String> item : service.getEnv().entrySet()) {
        env.put(item.getKey(), formatConfig(config, item.getValue()));
      }
    }

    for (int i = 0; i < service.getInstances(); i++) {
      addContainer(rmClient);
    }

    ctx = ContainerLaunchContext.newInstance(service.getLocalResources(),
                                             env,
                                             commands,
                                             null,
                                             tokens,
                                             null);
  }

  public boolean notifySet() {
    ready = numWaitingOn.decrementAndGet() == 0;
    return ready;
  }

  @SuppressWarnings("unchecked")
  public UUID addContainer(AMRMClientAsync rmClient) {
    ContainerRecord container;
    if (ready) {
      container = new ContainerRecord(name, ContainerState.WAITING);
      waiting.put(container.getId(), container);
    } else {
      container = new ContainerRecord(name, ContainerState.REQUESTED);
      requested.put(container.getId(), container);
      Priority priority = Priority.newInstance(0);
      rmClient.addContainerRequest(
          new ContainerRequest(service.getResources(), null, null, priority));
    }
    return container.getId();
  }

  public static enum ContainerState {
    WAITING,    // Waiting on service dependencies
    REQUESTED,  // Container requested, waiting to run
    RUNNING,    // Currently running
    FINISHED,   // Successfully finished running
    FAILED,     // Errored or was killed by yarn
    STOPPED     // Stopped by user
  }

  public static class ContainerRecord {
    private final String service;
    private final UUID id;
    private ContainerState state;
    private ContainerId containerId;

    public ContainerRecord(String service, ContainerState state) {
      this.service = service;
      this.id = UUID.randomUUID();
      this.state = state;
    }

    public String getService() { return service; }

    public UUID getId() { return id; }

    public ContainerState getState() { return state; }
    public void setState(ContainerState state) { this.state = state; }

    public ContainerId getContainerId() { return containerId; }
    public void setContainerId(ContainerId cid) { containerId = cid; }

    public String toString() {
      return ("ContainerRecord<"
              + "service: " + service + ", "
              + "id: " + id + ", "
              + "state: " + state + ", "
              + "containerId: " + containerId + ">");
    }
  }
}
