package com.anaconda.skein;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;

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

  private static void throwIfLessThan(int i, int min, String param)
      throws IllegalArgumentException {
    if (i < min) {
      throw new IllegalArgumentException(param + " must be > " + min + ", got " + i);
    }
  }

  public static class Service {
    private int instances;
    private int maxRestarts;
    private Resource resources;
    private Map<String, LocalResource> localResources;
    private Map<String, String> env;
    private List<String> commands;
    private Set<String> depends;

    public Service() {}

    public Service(int instances,
                   int maxRestarts,
                   Resource resources,
                   Map<String, LocalResource> localResources,
                   Map<String, String> env,
                   List<String> commands,
                   Set<String> depends) {
      this.instances = instances;
      this.maxRestarts = maxRestarts;
      this.resources = resources;
      this.localResources = localResources;
      this.env = env;
      this.commands = commands;
      this.depends = depends;
    }

    public String toString() {
      return ("Service:\n"
              + "instances: " + instances + "\n"
              + "maxRestarts: " + maxRestarts + "\n"
              + "resources: " + resources + "\n"
              + "localResources: " + localResources + "\n"
              + "env: " + env + "\n"
              + "commands: " + commands + "\n"
              + "depends: " + depends);
    }

    public void setInstances(int instances) { this.instances = instances; }
    public int getInstances() { return instances; }

    public void setMaxRestarts(int maxRestarts) { this.maxRestarts = maxRestarts; }
    public int getMaxRestarts() { return maxRestarts; }

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
      throwIfLessThan(instances, 0, "instances");
      throwIfLessThan(instances, -1, "maxRestarts");
      throwIfNull(resources, "resources");
      throwIfLessThan(resources.getMemory(), 1, "resources.memory");
      throwIfLessThan(resources.getVirtualCores(), 1, "resources.vcores");
      throwIfNull(localResources, "localResources");
      throwIfNull(env, "env");
      throwIfNull(commands, "commands");
      if (commands.size() == 0) {
        throw new IllegalArgumentException("There must be at least one command");
      }
      throwIfNull(depends, "depends");
    }
  }

  public static class ApplicationSpec {
    private String name;
    private String queue;
    private int maxAttempts;
    private Set<String> tags;
    private Map<String, Service> services;

    public ApplicationSpec() {}

    public ApplicationSpec(String name, String queue, int maxAttempts,
                           Set<String> tags, Map<String, Service> services) {
      this.name = name;
      this.queue = queue;
      this.maxAttempts = maxAttempts;
      this.tags = tags;
      this.services = services;
    }

    public String toString() {
      return ("ApplicationSpec<"
              + "name: " + name + ", "
              + "queue: " + queue + ", "
              + "maxAttempts: " + maxAttempts + ", "
              + "tags: " + tags + ", "
              + "services: " + services + ">");
    }

    public void setName(String name) { this.name = name; }
    public String getName() { return name; }

    public void setQueue(String queue) { this.queue = queue; }
    public String getQueue() { return queue; }

    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public int getMaxAttempts() { return maxAttempts; }

    public void setTags(Set<String> tags) { this.tags = tags; }
    public Set<String> getTags() { return this.tags; }

    public void setServices(Map<String, Service> services) { this.services = services; }
    public Map<String, Service> getServices() { return services; }

    public void validate() throws IllegalArgumentException {
      throwIfNull(name, "name");
      throwIfNull(queue, "queue");
      throwIfLessThan(maxAttempts, 1, "maxAttempts");
      throwIfNull(tags, "tags");
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
      KILLED
    }

    private String serviceName;
    private int instance;
    private State state;
    private ContainerId yarnContainerId;
    private NodeId yarnNodeId;
    private long startTime;
    private long finishTime;
    private ContainerRequest req;

    public Container() {}

    public Container(String serviceName, int instance, State state) {
      this.serviceName = serviceName;
      this.instance = instance;
      this.state = state;
      this.yarnContainerId = null;
      this.startTime = 0;
      this.finishTime = 0;
    }

    public String toString() {
      return ("Container<"
              + "serviceName: " + serviceName + ", "
              + "instance: " + instance + ">");
    }

    public void setServiceName(String serviceName) { this.serviceName = serviceName; }
    public String getServiceName() { return serviceName; }

    public void setInstance(int instance) { this.instance = instance; }
    public int getInstance() { return instance; }

    public void setState(State state) { this.state = state; }
    public State getState() { return state; }

    public void setYarnContainerId(ContainerId yarnContainerId) {
      this.yarnContainerId = yarnContainerId;
    }
    public ContainerId getYarnContainerId() { return yarnContainerId; }

    public void setYarnNodeId(NodeId yarnNodeId) { this.yarnNodeId = yarnNodeId; }
    public NodeId getYarnNodeId() { return yarnNodeId; }

    public void setStartTime(long startTime) { this.startTime = startTime; }
    public long getStartTime() { return startTime; }

    public void setFinishTime(long finishTime) { this.finishTime = finishTime; }
    public long getFinishTime() { return finishTime; }

    public void setContainerRequest(ContainerRequest req) { this.req = req; }
    public ContainerRequest popContainerRequest() {
      ContainerRequest out = this.req;
      this.req = null;
      return out;
    }
  }
}
