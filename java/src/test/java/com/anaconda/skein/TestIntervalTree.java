package com.anaconda.skein;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({TestIntervalTree.TestQuery.class,
                     TestIntervalTree.TestRemove.class,
                     TestIntervalTree.TestMisc.class})
public class TestIntervalTree {
  public static class TestQuery {
    IntervalTree<Integer> it;
    int numItems;

    @Before
    public void setUp() {
      it = new IntervalTree<Integer>();
      /*      -oo   a   b   c   d   e   f   g   h   i   oo
       *  ooc   |===|===|===|   |   |   |   |   |   |   |
       *  a2c   |   |===|===|   |   |   |   |   |   |   |
       *  a2e   |   |===|===|===|===|   |   |   |   |   |
       *  a2i   |   |===|===|===|===|===|===|   |   |   |
       *  b2d1  |   |   |===|===|   |   |   |   |   |   |
       *  b2d2  |   |   |===|===|   |   |   |   |   |   |
       *  d2f   |   |   |   |   |===|===|   |   |   |   |
       *  doo   |   |   |   |   |===|===|===|===|===|===|
       *  h2i   |   |   |   |   |   |   |   |   |===|   |
       */
      it.add(null, "c", 0);
      it.add("a", "c", 1);
      it.add("a", "e", 2);
      it.add("a", "g", 3);
      it.add("b", "d", 4);
      it.add("b", "d", 5);
      it.add("d", "f", 6);
      it.add("d", null, 7);
      it.add("h", "i", 8);
      numItems = 9;
    }

    @Test
    public void testQueryEmpty() {
      IntervalTree<Integer> it = new IntervalTree<Integer>();

      assertEquals(it.size(), 0);

      assertEquals(0, it.query(null, null).size());
      assertEquals(0, it.query("a").size());
    }

    @Test
    public void testQueryFullyInsideBounds() {
      assertEquals(numItems, it.size());

      assertEquals(1, it.query(null, "\u0000").size());
      assertEquals(4, it.query("aa", "aaa").size());
      assertEquals(6, it.query("bb", "bbb").size());
      assertEquals(4, it.query("cc", "ccc").size());
      assertEquals(4, it.query("dd", "ddd").size());
      assertEquals(3, it.query("ee", "eee").size());
      assertEquals(2, it.query("ff", "fff").size());
      assertEquals(1, it.query("gg", "ggg").size());
      assertEquals(2, it.query("hh", "hhh").size());
      assertEquals(1, it.query("ii", null).size());
    }

    @Test
    public void testQueryBoundsLeft() {
      assertEquals(numItems, it.size());

      assertEquals(4, it.query("a", "aa").size());
      assertEquals(4, it.query("c", "cc").size());
      assertEquals(1, it.query("g", "gg").size());
    }

    @Test
    public void testQueryBoundsRight() {
      assertEquals(numItems, it.size());

      assertEquals(6, it.query("aa", "b").size());
      assertEquals(6, it.query("cc", "d").size());
      assertEquals(2, it.query("gg", "h").size());
      assertEquals(1, it.query("ii", null).size());
    }

    @Test
    public void testQuerySingleTarget() {
      assertEquals(numItems, it.size());

      assertEquals(4, it.query("a").size());
      assertEquals(4, it.query("aa").size());
      assertEquals(6, it.query("b").size());
      assertEquals(2, it.query("f").size());
      assertEquals(1, it.query("g").size());
    }
  }

  public static class TestRemove {
    @Test
    public void testRemoveNonExistant() {
      IntervalTree<Integer> it = new IntervalTree<Integer>();

      assertEquals(it.size(), 0);
      assertFalse(it.remove(100));
      assertEquals(it.size(), 0);
    }

    @Test
    public void testRemove() {
      IntervalTree<Integer> it = new IntervalTree<Integer>();

      assertEquals(0, it.size());

      int ac1 = it.add("a", "c", 1);
      int ac2 = it.add("a", "c", 2);
      int ad = it.add("a", "d", 3);
      int cd = it.add("c", "d", 1);

      assertEquals(4, it.size());
      assertEquals(3, it.query(null, "b").size());

      // Delete a node, check that size and queries change
      assertTrue(it.remove(ad));
      assertEquals(3, it.size());
      assertEquals(2, it.query(null, "b").size());

      // Delete the node again, no-op
      assertFalse(it.remove(ad));
      assertEquals(3, it.size());

      // Re-add an equivalent node. Check new id, and queries work
      int ad2 = it.add("a", "d", 3);
      assertFalse(ad == ad2);
      assertEquals(4, it.size());
      assertEquals(3, it.query(null, "b").size());

      // Remove items from node with multiple values
      assertTrue(it.remove(ac1));
      assertEquals(3, it.size());
      assertEquals(2, it.query(null, "b").size());
      assertFalse(it.remove(ac1));

      assertTrue(it.remove(ac2));
      assertEquals(2, it.size());
      assertEquals(1, it.query(null, "b").size());
      assertFalse(it.remove(ac2));
    }
  }

  public static class TestMisc {
    @Test
    public void testNullStringCompare() {
      assertEquals(IntervalTree.nullStringCompare(null, null), 0);
      assertEquals(IntervalTree.nullStringCompare(null, "a"), 1);
      assertEquals(IntervalTree.nullStringCompare("a", null), -1);
      assertEquals(IntervalTree.nullStringCompare("a", "a"), 0);
      assertEquals(IntervalTree.nullStringCompare("b", "a"), 1);
      assertEquals(IntervalTree.nullStringCompare("a", "b"), -1);
    }
  }
}
