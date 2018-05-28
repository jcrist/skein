package com.anaconda.skein;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class Model {
  private static void throwIfNull(Object obj, String param)
      throws IllegalArgumentException {
    if (obj == null) {
      throw new IllegalArgumentException(param + " must be non-null");
    }
  }

  private static void throwIfNonPositive(int i, String param)
      throws IllegalArgumentException {
    if (i <= 0) {
      throw new IllegalArgumentException(param + " must be > 0");
    }
  }

  public static class Service {
    private int instances;
    private Resource resources;
    private Map<String, LocalResource> localResources;
    private Map<String, String> env;
    private List<String> commands;
    private Set<String> depends;

    public Service() {}

    public Service(int instances, Resource resources,
                   Map<String, LocalResource> localResources,
                   Map<String, String> env, List<String> commands,
                   Set<String> depends) {
      this.instances = instances;
      this.resources = resources;
      this.localResources = localResources;
      this.env = env;
      this.commands = commands;
      this.depends = depends;
    }

    public String toString() {
      return ("Service:\n"
              + "instances: " + instances + "\n"
              + "resources: " + resources + "\n"
              + "localResources: " + localResources + "\n"
              + "env: " + env + "\n"
              + "commands: " + commands + "\n"
              + "depends: " + depends);
    }

    public void setInstances(int instances) { this.instances = instances; }
    public int getInstances() { return instances; }

    public void setResources(Resource resources) { this.resources = resources; }
    public Resource getResources() { return resources; }

    public void setLocalResources(Map<String, LocalResource> r) { this.localResources = r; }
    public Map<String, LocalResource> getLocalResources() { return localResources; }

    public void setEnv(Map<String, String> env) { this.env = env; }
    public Map<String, String> getEnv() { return env; }

    public void setCommands(List<String> commands) { this.commands = commands; }
    public List<String> getCommands() { return commands; }

    public void setDepends(Set<String> depends) { this.depends = depends; }
    public Set<String> getDepends() { return depends; }

    public void validate() throws IllegalArgumentException {
      throwIfNonPositive(instances, "instances");
      throwIfNull(resources, "resources");
      throwIfNonPositive(resources.getMemory(), "resources.memory");
      throwIfNonPositive(resources.getVirtualCores(), "resources.vcores");
      throwIfNull(localResources, "localResources");
      throwIfNull(env, "env");
      throwIfNull(commands, "commands");
      if (commands.size() == 0) {
        throw new IllegalArgumentException("There must be at least one command");
      }
      throwIfNull(depends, "depends");
    }
  }

  public static class Job {
    private String name;
    private String queue;
    private Map<String, Service> services;

    public Job() {}

    public Job(String name, String queue, Map<String, Service> services) {
      this.name = name;
      this.queue = queue;
      this.services = services;
    }

    public String toString() {
      return ("Job<"
              + "name: " + name + ", "
              + "queue: " + queue + ", "
              + "services: " + services + ">");
    }

    public void setName(String name) { this.name = name; }
    public String getName() { return name; }

    public void setQueue(String queue) { this.queue = queue; }
    public String getQueue() { return queue; }

    public void setServices(Map<String, Service> services) { this.services = services; }
    public Map<String, Service> getServices() { return services; }

    public void validate() throws IllegalArgumentException {
      throwIfNull(name, "name");
      throwIfNull(queue, "queue");
      throwIfNull(services, "services");
      if (services.size() == 0) {
        throw new IllegalArgumentException("There must be at least one service");
      }
      for (Service s: services.values()) {
        s.validate();
      }
    }
  }

  public static class Container {
    public enum State {
      WAITING,
      REQUESTED,
      RUNNING,
      SUCCEEDED,
      FAILED,
      STOPPED
    }

    private String serviceName;
    private String id;
    private State state;
    private ContainerId containerId;
    private NodeId nodeId;
    private long startTime;
    private long finishTime;

    public Container() {}

    public Container(String serviceName, String id, State state) {
      this.serviceName = serviceName;
      this.id = id;
      this.state = state;
      this.containerId = null;
      this.startTime = 0;
      this.finishTime = 0;
    }

    public String toString() {
      return ("Container<"
              + "serviceName: " + serviceName + ", "
              + "id: " + id + ">");
    }

    public void setServiceName(String serviceName) { this.serviceName = serviceName; }
    public String getServiceName() { return serviceName; }

    public void setId(String id) { this.id = id; }
    public String getId() { return id; }

    public void setState(State state) { this.state = state; }
    public State getState() { return state; }

    public void setContainerId(ContainerId containerId) { this.containerId = containerId; }
    public ContainerId getContainerId() { return containerId; }

    public void setNodeId(NodeId nodeId) { this.nodeId = nodeId; }
    public NodeId getNodeId() { return nodeId; }

    public void setStartTime(long startTime) { this.startTime = startTime; }
    public long getStartTime() { return startTime; }

    public void setFinishTime(long finishTime) { this.finishTime = finishTime; }
    public long getFinishTime() { return finishTime; }
  }
}
