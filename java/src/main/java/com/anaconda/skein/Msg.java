package com.anaconda.skein;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Msg {
  public static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    SimpleModule mod = new SimpleModule("skein");
    mod.addDeserializer(Resource.class, new ResourceDeserializer());
    mod.addSerializer(Resource.class, new ResourceSerializer());
    mod.addDeserializer(LocalResource.class, new LocalResourceDeserializer());
    mod.addSerializer(LocalResource.class, new LocalResourceSerializer());
    MAPPER.registerModule(mod);
  }

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

  private static void checkJsonKeys(JsonNode node, String [] valid,
                                    JsonParser jp, Class cls) throws IOException {
    if (!node.isObject()) {
      throw MismatchedInputException.from(jp, cls, "Expected Object");
    }

    for (String prop : valid) {
      if (!node.hasNonNull(prop)) {
        throw MismatchedInputException.from(jp, cls, prop + " must be non-null");
      }
    }

    if (node.size() > valid.length) {
      for (Iterator<String> iter = node.fieldNames(); iter.hasNext();) {
        String prop = iter.next();
        for (String ok : valid) {
          if (!prop.equals(ok)) {
            throw UnrecognizedPropertyException.from(jp, cls, prop, null);
          }
        }
      }
    }
  }

  private static enum TYPE { INT, LONG, TEXT, OBJECT }

  private static void checkJsonType(JsonNode node, String prop, TYPE type,
                                    JsonParser jp, Class cls) throws IOException {
    JsonNode field = node.get(prop);
    boolean ok = true;
    switch (type) {
      case INT: ok = field.canConvertToInt();
                break;
      case LONG: ok = field.canConvertToLong();
                 break;
      case TEXT: ok = field.isTextual();
                 break;
      case OBJECT: ok = field.isObject();
                   break;
    }
    if (!ok) {
      throw MismatchedInputException.from(jp, cls,
          prop + " must be of type " + type.toString() + " -- " + field);
    }
  }

  private static class ResourceDeserializer extends StdDeserializer<Resource> {

    private static final String[] VALID = new String[] {"vcores", "memory"};

    public ResourceDeserializer() {
      this(null);
    }

    public ResourceDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Resource deserialize(JsonParser jp, DeserializationContext ctx)
          throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);

      checkJsonKeys(node, VALID, jp, Resource.class);
      checkJsonType(node, "memory", TYPE.INT, jp, Resource.class);
      checkJsonType(node, "vcores", TYPE.INT, jp, Resource.class);

      int memory = node.get("memory").asInt();
      int vcores = node.get("vcores").asInt();

      return Resource.newInstance(memory, vcores);
    }
  }

  private static class ResourceSerializer extends JsonSerializer<Resource> {
    @Override
    public void serialize(Resource r, JsonGenerator jg, SerializerProvider s)
          throws IOException {
      jg.writeStartObject();
      jg.writeNumberField("memory", r.getMemory());
      jg.writeNumberField("vcores", r.getVirtualCores());
      jg.writeEndObject();
    }
  }

  private static class LocalResourceDeserializer extends StdDeserializer<LocalResource> {

    private static final String[] VALID = new String[] {"url", "type", "size", "timestamp"};
    private static final String[] URL_VALID = new String[] {"scheme", "host", "port", "file"};

    public LocalResourceDeserializer() {
      this(null);
    }

    public LocalResourceDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public LocalResource deserialize(JsonParser jp, DeserializationContext ctx)
          throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);

      checkJsonKeys(node, VALID, jp, LocalResource.class);
      checkJsonType(node, "url", TYPE.OBJECT, jp, LocalResource.class);
      checkJsonType(node, "type", TYPE.TEXT, jp, LocalResource.class);
      checkJsonType(node, "size", TYPE.LONG, jp, LocalResource.class);
      checkJsonType(node, "timestamp", TYPE.LONG, jp, LocalResource.class);

      JsonNode urlNode = node.get("url");
      checkJsonKeys(urlNode, URL_VALID, jp, URL.class);
      checkJsonType(urlNode, "scheme", TYPE.TEXT, jp, URL.class);
      checkJsonType(urlNode, "host", TYPE.TEXT, jp, URL.class);
      checkJsonType(urlNode, "port", TYPE.INT, jp, URL.class);
      checkJsonType(urlNode, "file", TYPE.TEXT, jp, URL.class);

      URL url = URL.newInstance(urlNode.get("scheme").asText(),
                                urlNode.get("host").asText(),
                                urlNode.get("port").asInt(),
                                urlNode.get("file").asText());

      LocalResourceType type;
      try {
        type = LocalResourceType.valueOf(node.get("type").asText());
      } catch (IllegalArgumentException exc) {
        throw MismatchedInputException.from(jp, LocalResource.class,
            "type must be either FILE, ARCHIVE, or PATTERN");
      }

      return LocalResource.newInstance(url, type,
                                       LocalResourceVisibility.APPLICATION,
                                       node.get("size").asLong(),
                                       node.get("timestamp").asLong());
    }
  }

  private static class LocalResourceSerializer extends JsonSerializer<LocalResource> {
    @Override
    public void serialize(LocalResource r, JsonGenerator jg, SerializerProvider s)
          throws IOException {
      jg.writeStartObject();

      URL url = r.getResource();
      jg.writeFieldName("url");
      jg.writeStartObject();
      jg.writeStringField("scheme", url.getScheme());
      jg.writeStringField("host", url.getHost());
      jg.writeNumberField("port", url.getPort());
      jg.writeStringField("file", url.getFile());
      jg.writeEndObject();

      jg.writeStringField("type", r.getType().toString());
      jg.writeNumberField("size", r.getSize());
      jg.writeNumberField("timestamp", r.getTimestamp());
      jg.writeEndObject();
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
      throwIfNonPositive(resources.getMemory(), "resources.memory");
      throwIfNonPositive(resources.getVirtualCores(), "resources.vcores");

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
      throwIfNull(services, "services");
      if (services.size() == 0) {
        throw new IllegalArgumentException("There must be at least one service");
      }
      for (Service s: services.values()) {
        s.validate();
      }
    }
  }
}
