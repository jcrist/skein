package com.anaconda.skein;

import java.util.List;
import java.util.Map;

public class Spec {

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
  }

  public static class Service {
    private int instances;
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

    public int getInstances() {
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
  }
}
