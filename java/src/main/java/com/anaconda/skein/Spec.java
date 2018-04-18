package com.anaconda.skein;

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

  public static class Resource {
    private Integer vcores;
    private Integer memory;

    public String toString() {
      return ("Resource<"
              + "vcores=" + vcores + ", "
              + "memory=" + memory + ">");
    }

    public Integer getVcores() {
      return vcores;
    }

    public Integer getMemory() {
      return memory;
    }

    public void validate() throws IllegalArgumentException {
      throwIfNull(vcores, "vcores");
      throwIfNonPositive(vcores, "vcores");

      throwIfNull(memory, "memory");
      throwIfNonPositive(memory, "memory");
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
      if (!kind.equals("FILE") || !kind.equals("ARCHIVE")) {
        throw new IllegalArgumentException("kind must be either FILE or ARCHIVE");
      }
    }
  }

  public static class Service {
    private Integer instances;
    private Resource resources;
    private List<File> files;
    private Map<String, String> env;
    private List<String> commands;
    private List<String> depends;

    public String toString() {
      return ("Service:\n"
              + "instances: " + instances + "\n"
              + "resources: " + resources + "\n"
              + "files: " + files + "\n"
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

    public Map<String, String> getEnv() {
      return env;
    }

    public List<String> getCommands() {
      return commands;
    }

    public List<String> getDepends() {
      return depends;
    }

    public void validate() throws IllegalArgumentException {
      throwIfNull(instances, "instances");
      throwIfNonPositive(instances, "instances");

      throwIfNull(resources, "resources");
      resources.validate();

      if (files != null) {
        for (File f: files) {
          f.validate();
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

    public void validate() throws IllegalArgumentException {
      throwIfNull(name, "name");
      throwIfNull(queue, "queue");

      if (services != null) {
        for (Service s: services.values()) {
          s.validate();
        }
      }
    }
  }
}
