package com.anaconda.skein;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import junit.framework.TestCase;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SkeinObjectMapperTest extends TestCase {

  private void assertRaisesOnParse(String msg, Class<?> cls) throws Exception {
    try {
      Utils.MAPPER.readValue(msg.getBytes(), cls);
      fail("Should have raised when parsing " + cls + "\n\nmsg: " + msg);
    } catch (MismatchedInputException exc) {
    }
  }

  private JsonNode makeUrl(String scheme, String host, Integer port, String file)
      throws IOException {
    ObjectNode node = Utils.MAPPER.createObjectNode();
    node.put("scheme", scheme);
    node.put("host", host);
    node.put("port", port);
    node.put("file", file);
    return node;
  }

  private String makeLocal(JsonNode url, String type, Integer size, Integer timestamp)
      throws IOException {
    ObjectNode node = Utils.MAPPER.createObjectNode();
    node.set("url", url);
    node.put("type", type);
    node.put("size", size);
    node.put("timestamp", timestamp);
    return Utils.MAPPER.writeValueAsString(node);
  }

  public void testRoundtripResource() throws Exception {
    Resource r = Resource.newInstance(100, 200);
    byte[] msg = Utils.MAPPER.writeValueAsBytes(r);
    Resource r2 = Utils.MAPPER.readValue(msg, Resource.class);

    assertEquals(r, r2);
  }

  public void testResourceExceptions() throws Exception {
    for (String msg : Arrays.asList(
        "1",
        "{}",
        "{\"vcores\": 1, \"memory\": 2, \"a\": 3}",
        "{\"memory\": 100}",
        "{\"vcores\": 100}",
        "{\"vcores\": 1, \"memory\": null}",
        "{\"vcores\": \"foo\", \"memory\": 2}"
        )) {
      assertRaisesOnParse(msg, Resource.class);
    }
  }

  public void testRoundtripLocalResource() throws Exception {
    URL url = URL.newInstance("hdfs", "master.example.com", 9000,
                              "/foo/bar.txt");
    LocalResource r = LocalResource.newInstance(url, LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION, 1234, 5678);

    byte[] msg = Utils.MAPPER.writeValueAsBytes(r);
    LocalResource r2 = Utils.MAPPER.readValue(msg, LocalResource.class);

    assertEquals(r, r2);
  }

  public void testLocalResourceExceptions() throws Exception {
    JsonNode goodurl = makeUrl("hdfs", "master", 1234, "foo.txt");
    List<JsonNode> badurls = Arrays.asList(
        null,
        Utils.MAPPER.createObjectNode(),
        Utils.MAPPER.valueToTree(1),
        makeUrl(null, "master", 1234, "foo.txt"),
        makeUrl("hdfs", null, 1234, "foo.txt"),
        makeUrl("hdfs", "master", null, "foo.txt"),
        makeUrl("hdfs", "master", 1234, null));

    for (JsonNode url: badurls) {
      assertRaisesOnParse(makeLocal(url, "FILE", 123, 456),
                          LocalResource.class);
    }

    for (String msg : Arrays.asList(
        makeLocal(goodurl, "INVALID", 123, 456),
        makeLocal(goodurl, null, 123, 456),
        makeLocal(goodurl, "FILE", null, 456),
        makeLocal(goodurl, "FILE", 123, null))) {
      assertRaisesOnParse(msg, LocalResource.class);
    }
  }
}
