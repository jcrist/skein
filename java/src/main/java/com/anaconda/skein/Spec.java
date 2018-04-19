package com.anaconda.skein;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.List;
import java.util.Map;

public class Spec {
  private static void throwIfNull(Object obj, String param)
      throws IllegalArgumentException {
    if (obj == null) {
      throw new IllegalArgumentException(param + " must be non-null");
    }
  }

  private static void throwIfNonPositive(Integer i, String param)
      throws IllegalArgumentException {
    if (i <= 0) {
      throw new IllegalArgumentException(param + " must be > 0");
    }
  }

  public static class File {
    private String source;
    private String dest;
    private String kind;

    public String toString() {
      return ("File<"
              + "source=" + source + ", "
              + "dest=" + dest + ", "
              + "kind=" + kind + ">");
    }

    public String getSource() {
      return source;
    }

    public String getDest() {
      return dest;
    }

    public String getKind() {
      return kind;
    }

    public void validate() throws IllegalArgumentException {
      throwIfNull(source, "source");
      throwIfNull(dest, "dest");
      throwIfNull(kind, "kind");
      if (!(kind.equals("FILE") || kind.equals("ARCHIVE"))) {
        throw new IllegalArgumentException("kind must be either FILE or ARCHIVE");
      }
    }
  }

  public static class Service {
    private Integer instances;
    private Resource resources;

    // User -> Client: files is set, localResources is null
    // Client -> ApplicationMaster: localResources is set, files is null
    private List<File> files;
    private Map<String, LocalResource> localResources;

    private Map<String, String> env;
    private List<String> commands;
    private List<String> depends;

    public String toString() {
      return ("Service:\n"
              + "instances: " + instances + "\n"
              + "resources: " + resources + "\n"
              + "files: " + files + "\n"
              + "localResources: " + localResources + "\n"
              + "env: " + env + "\n"
              + "commands: " + commands + "\n"
              + "depends: " + depends);
    }

    public Integer getInstances() {
      return instances;
    }

    public Resource getResources() {
      return resources;
    }

    public List<File> getFiles() {
      return files;
    }

    public Map<String, LocalResource> getLocalResources() {
      return localResources;
    }

    public Map<String, String> getEnv() {
      return env;
    }

    public List<String> getCommands() {
      return commands;
    }

    public List<String> getDepends() {
      return depends;
    }

    public void validate(boolean uploaded) throws IllegalArgumentException {
      throwIfNull(instances, "instances");
      throwIfNonPositive(instances, "instances");

      throwIfNull(resources, "resources");
      throwIfNonPositive(resources.getMemory(), "resources.memory");
      throwIfNonPositive(resources.getVirtualCores(), "resources.vcores");

      if (uploaded) {
        if (files != null) {
          throw new IllegalArgumentException("unexpected field: files");
        }
      } else {
        if (localResources != null) {
          throw new IllegalArgumentException("unexpected field: localResources");
        }
        if (files != null) {
          for (File f : files) {
            f.validate();
          }
        }
      }
    }
  }

  public static class Job {
    private String name;
    private String queue;
    private Map<String, Service> services;

    public String toString() {
      return ("Job<"
              + "name: " + name + ", "
              + "queue: " + queue + ", "
              + "services: " + services + ">");
    }

    public String getName() {
      return name;
    }

    public String getQueue() {
      return queue;
    }

    public Map<String, Service> getServices() {
      return services;
    }

    public void validate(boolean uploaded) throws IllegalArgumentException {
      throwIfNull(name, "name");
      throwIfNull(queue, "queue");
      throwIfNull(services, "services");
      if (services.size() == 0) {
        throw new IllegalArgumentException("There must be at least one service");
      }
      for (Service s: services.values()) {
        s.validate(uploaded);
      }
    }
  }
}
