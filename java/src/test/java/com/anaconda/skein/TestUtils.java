package com.anaconda.skein;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.List;

public class TestUtils {
  @Test
  public void testFormatAcl() {
    List<String> empty = Lists.newArrayList();
    List<String> star = Lists.newArrayList("*");
    List<String> hasStar = Lists.newArrayList("other", "*");

    assertEquals(Utils.formatAcl(empty, star), "*");
    assertEquals(Utils.formatAcl(star, empty), "*");
    assertEquals(Utils.formatAcl(empty, hasStar), "*");
    assertEquals(Utils.formatAcl(hasStar, empty), "*");
    assertEquals(Utils.formatAcl(empty, empty), " ");

    assertEquals(Utils.formatAcl(Lists.newArrayList("a", "b"), empty), "a,b ");
    assertEquals(Utils.formatAcl(empty, Lists.newArrayList("a", "b")), " a,b");
    assertEquals(Utils.formatAcl(Lists.newArrayList("a", "b"),
                                 Lists.newArrayList("c", "d")),
                 "a,b c,d");
  }
}
