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
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;

import java.io.IOException;
import java.util.Iterator;

public class SkeinObjectMapper extends ObjectMapper {
  public SkeinObjectMapper() {
    registerModule(new SkeinModule());
  }

  private static class SkeinModule extends SimpleModule {
    public SkeinModule() {
      super("SkeinModule");
      addDeserializer(Resource.class, new ResourceDeserializer());
      addSerializer(Resource.class, new ResourceSerializer());
      addDeserializer(LocalResource.class, new LocalResourceDeserializer());
      addSerializer(LocalResource.class, new LocalResourceSerializer());
      addSerializer(ApplicationReport.class, new ApplicationReportSerializer());
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

  private static class ApplicationReportSerializer
      extends JsonSerializer<ApplicationReport> {
    @Override
    public void serialize(ApplicationReport r, JsonGenerator jg,
                          SerializerProvider s)
          throws IOException {

      jg.writeStartObject();
      jg.writeStringField("id", r.getApplicationId().toString());
      jg.writeStringField("name", r.getName());
      jg.writeStringField("user", r.getUser());
      jg.writeStringField("queue", r.getQueue());
      jg.writeStringField("host", r.getHost());
      jg.writeNumberField("port", r.getRpcPort());
      jg.writeStringField("trackingUrl", r.getTrackingUrl());
      jg.writeStringField("state", r.getYarnApplicationState().toString());
      jg.writeStringField("finalStatus", r.getFinalApplicationStatus().toString());
      jg.writeNumberField("progress", r.getProgress());
      jg.writeStringField("diagnostics", r.getDiagnostics());
      jg.writeNumberField("startTime", r.getStartTime());
      jg.writeNumberField("finishTime", r.getFinishTime());
      s.defaultSerializeField("tags", r.getApplicationTags(), jg);

      ApplicationResourceUsageReport usage = r.getApplicationResourceUsageReport();
      jg.writeObjectFieldStart("usage");
      jg.writeNumberField("memorySeconds", usage.getMemorySeconds());
      jg.writeNumberField("vcoreSeconds", usage.getVcoreSeconds());
      jg.writeNumberField("numUsedContainers", usage.getNumUsedContainers());
      s.defaultSerializeField("reservedResources", usage.getReservedResources(), jg);
      s.defaultSerializeField("usedResources", usage.getUsedResources(), jg);
      s.defaultSerializeField("neededResources", usage.getNeededResources(), jg);
      jg.writeEndObject(); // usage

      jg.writeEndObject(); // application report
    }
  }
}
