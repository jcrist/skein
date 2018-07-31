/* The following file was copied and *heavily* modified from
 *
 * https://github.com/tinloaf/intervaltree/blob/master/src/de/tinloaf/intervaltree/IntervalTree.java
 *
 * In particular, the following changes were made:
 *
 * - Support left inclusive, right exclusive bounds
 * - Specialize only for our use case
 * - Remove unnecessary methods
 * - Simplify and cleanup code
 *
 * The original source was copyrighted under the MIT license, (c) 2014 tinloaf.
 * The original license is included in a comment at the end of this file.
 */

package com.anaconda.skein;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class IntervalTree<T> {
  private TreeNode<T> root;
  private Map<Integer, Item<T>> lookup;
  private int currentId = 0;

  private static enum Color {
    RED, BLACK
  }

  public IntervalTree() {
    this.root = null;
    this.lookup = new HashMap<Integer, Item<T>>();
  }

  private int nextId() {
    return currentId++;
  }

  public int size() {
    return lookup.size();
  }

  public static class Interval {
    private String begin;
    private String end;

    Interval(String begin, String end) {
      // For both begin and end, "" and null are equivalent.
      // For begin, we store "", for end we store null
      // This simplifies comparisons later on.
      this.begin = begin == null ? "" : begin;
      this.end = (end != null && end.isEmpty()) ? null : end;
    }

    String getBegin() { return begin; }
    String getEnd() { return end; }

    @Override
    public int hashCode() {
      int hash = begin == null ? 0 : begin.hashCode();
      return 31 * hash + (end == null ? 0 : end.hashCode());
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other == null || this.getClass() != other.getClass()) {
        return false;
      }

      Interval iother = (Interval) other;

      return ((begin == null ? iother.begin == null : begin.equals(iother.begin))
              && (end == null) ? iother.end == null : end.equals(iother.end));
    }
  }

  public static class Item<V> {
    private Interval interval;
    private TreeNode<V> treenode;
    private V value;
    private int id;

    Item(Interval interval, V value, int id) {
      this.interval = interval;
      this.value = value;
      this.id = id;
    }

    Item(String begin, String end, V value, int id) {
      this(new Interval(begin, end), value, id);
    }

    public Interval getInterval() { return interval; }
    public int getId() { return id; }
    public V getValue() { return value; }
    public String getIntervalBegin() { return interval.getBegin(); }
    public String getIntervalEnd() { return interval.getEnd(); }
  }

  private static class TreeNode<V> {
    Interval interval;
    Color color;
    TreeNode<V> left;
    TreeNode<V> right;
    TreeNode<V> parent;
    HashSet<Item<V>> items;
    String max;

    TreeNode(Interval interval, Color nodeColor, TreeNode<V> left, TreeNode<V> right) {
      this.interval = interval;
      this.color = nodeColor;
      this.left = left;
      this.right = right;
      this.parent = null;
      this.items = new HashSet<Item<V>>();
      updateMax();
    }

    void updateMax() {
      max = interval.end;

      if (left != null) {
        left.parent = this;
        max = nullStringMax(max, left.max);
      }

      if (right != null) {
        right.parent = this;
        max = nullStringMax(max, right.max);
      }
    }

    TreeNode<V> grandparent() {
      return parent.parent;
    }

    TreeNode<V> sibling() {
      return (this == parent.right) ? parent.left : parent.right;
    }

    TreeNode<V> uncle() {
      return parent.sibling();
    }

    private List<Item<V>> query(Interval target, List<Item<V>> out) {
      boolean eRightOfB = nullStringCompare(interval.begin, target.end) <= 0;

      if (eRightOfB && nullStringCompare(interval.end, target.begin) > 0) {
        out.addAll(items);
      }

      if (left != null && nullStringCompare(left.max, target.begin) > 0) {
        left.query(target, out);
      }

      if (right != null && eRightOfB) {
        right.query(target, out);
      }

      return out;
    }

    private List<Item<V>> query(String target, List<Item<V>> out) {
      boolean rightOfB = nullStringCompare(interval.begin, target) <= 0;

      if (rightOfB && nullStringCompare(interval.end, target) > 0) {
        out.addAll(items);
      }

      if (left != null && nullStringCompare(left.max, target) > 0) {
        left.query(target, out);
      }

      if (right != null && rightOfB) {
        right.query(target, out);
      }

      return out;
    }

  }

  public static int nullStringCompare(String a, String b) {
    if (a == null) {
      return b == null ? 0 : 1;
    } else if (b == null) {
      return -1;
    } else {
      return a.compareTo(b);
    }
  }

  private static String nullStringMax(String a, String b) {
    return nullStringCompare(a, b) < 0 ? b : a;
  }

  private Color nodeColor(TreeNode<T> n) {
    return (n == null) ? Color.BLACK : n.color;
  }

  // Find all intervals containing the target
  public List<Item<T>> query(String target) {
    if (root == null) {
      return Collections.emptyList();
    }
    return root.query(target, new ArrayList<Item<T>>(Math.round(size() * 0.5f)));
  }

  // Find all intervals intersecting with [begin, end] (note inclusive end).
  public List<Item<T>> query(String begin, String end) {
    if (root == null) {
      return Collections.emptyList();
    }
    return root.query(new Interval(begin, end),
                      new ArrayList<Item<T>>(Math.round(size() * 0.5f)));
  }

  private void replaceNode(TreeNode<T> oldn, TreeNode<T> newn) {
    if (oldn.parent == null) {
      root = newn;
    } else {
      if (oldn == oldn.parent.left) {
        oldn.parent.left = newn;
      } else {
        oldn.parent.right = newn;
      }
    }
    if (newn != null) {
      newn.parent = oldn.parent;
    }
  }

  private void rotateLeft(TreeNode<T> n) {
    TreeNode<T> r = n.right;
    replaceNode(n, r);
    n.right = r.left;
    if (r.left != null) {
      r.left.parent = n;
    }
    r.left = n;
    n.parent = r;

    // Update max towards root
    n.updateMax();
    r.updateMax();
    if (r.parent != null) {
      r.parent.updateMax();
    }
  }

  private void rotateRight(TreeNode<T> n) {
    TreeNode<T> l = n.left;
    replaceNode(n, l);
    n.left = l.right;
    if (l.right != null) {
      l.right.parent = n;
    }
    l.right = n;
    n.parent = l;

    // Update max towards root
    n.updateMax();
    l.updateMax();
    if (l.parent != null) {
      l.parent.updateMax();
    }
  }

  public int add(String begin, String end, T value) {
    Item<T> item = new Item<T>(begin, end, value, nextId());
    TreeNode<T> newNode = new TreeNode<T>(item.interval, Color.RED, null, null);
    lookup.put(item.id, item);

    if (root == null) {
      root = newNode;
    } else {
      TreeNode<T> n = root;
      while (true) {
        int beginComp = nullStringCompare(item.interval.begin, n.interval.begin);
        n.max = nullStringMax(n.max, item.interval.end);

        if (beginComp == 0) {
          int endComp = nullStringCompare(item.interval.end, n.interval.end);
          if (endComp == 0) {
            // Match with existing interval, just add to set
            n.items.add(item);
            item.treenode = n;
            return item.id;
          } else {
            if (endComp > 0) {
              if (n.right == null) {
                n.right = newNode;
                break;
              } else {
                n = n.right;
              }
            } else {
              if (n.left == null) {
                n.left = newNode;
                break;
              } else {
                n = n.left;
              }
            }
          }
        } else if (beginComp < 0) {
          if (n.left == null) {
            n.left = newNode;
            break;
          } else {
            n = n.left;
          }
        } else {
          if (n.right == null) {
            n.right = newNode;
            break;
          } else {
            n = n.right;
          }
        }
      }
      newNode.parent = n;
    }

    newNode.items.add(item);
    item.treenode = newNode;
    fixTreeAt(newNode);
    return item.id;
  }

  private void fixTreeAt(TreeNode<T> n) {
    /*
     * Case 1: We are the root. We are black, everything else is still OK
     */
    if (n.parent == null) {
      n.color = Color.BLACK;
      return;
    }

    /*
     * Case 2: Our parent is black. Two consecutive blacks are allowed, all is
     * well.
     */
    if (nodeColor(n.parent) == Color.BLACK) {
      return; // Tree is still valid
    }

    /*
     * Case 3: We violate red-red, but our uncle is red, too. We switch the
     * colors of (uncle/parent) and grandparent and restart fixing at our
     * grandparent.
     */
    if (nodeColor(n.uncle()) == Color.RED) {
      n.parent.color = Color.BLACK;
      n.uncle().color = Color.BLACK;
      n.grandparent().color = Color.RED;
      fixTreeAt(n.grandparent());
      return;
    }

    /*
     * Case 4: We violate red-red, and our uncle is black. We need to rotate!
     */

    /*
     * Sub-Case 4.1: We are "inside" our grandparents subtree, i.e. we are the
     * left child and our parent is a right child (or vice versa). We need to
     * rotate ourselves to the outside (by rotating about our parent) to apply
     * the fix for case 4.
     */
    if (n == n.parent.right && n.parent == n.grandparent().left) {
      rotateLeft(n.parent);
      n = n.left;
    } else if (n == n.parent.left && n.parent == n.grandparent().right) {
      rotateRight(n.parent);
      n = n.right;
    }

    /*
     * Now, finally fix case 4 by rotating about our parent.
     */
    n.parent.color = Color.BLACK;
    n.grandparent().color = Color.RED;
    if (n == n.parent.left && n.parent == n.grandparent().left) {
      rotateRight(n.grandparent());
    } else {
      assert n == n.parent.right && n.parent == n.grandparent().right;
      rotateLeft(n.grandparent());
    }
  }

  public boolean remove(int id) {
    Item<T> item = lookup.remove(id);

    if (item == null) {
      return false;  // Key not found, do nothing
    }

    TreeNode<T> n = item.treenode;

    if (n.items.size() > 1) {
      // we retain the node since it contains multiple items
      n.items.remove(item);
      return true;
    }

    if (n.left != null && n.right != null) {
      // Copy key/value from predecessor and then delete it instead
      TreeNode<T> pred = maximumNode(n.left);
      n.interval = pred.interval;
      n.items = pred.items;
      for (Item<T> it : n.items) {
        it.treenode = n;
      }
      n = pred;
    }

    TreeNode<T> child = (n.right == null) ? n.left : n.right;

    if (nodeColor(n) == Color.BLACK) {
      n.color = nodeColor(child);
      fixTreeForDeletion(n);
    }
    replaceNode(n, child);

    if ((child != null) && (child.parent == null)) {
      child.color = Color.BLACK;
    }

    // Fix all the max values from what we just deleted up to the root
    TreeNode<T> cur = n.parent;
    while (cur != null) {
      cur.updateMax();
      cur = cur.parent;
    }
    return true;
  }

  private void fixTreeForDeletion(TreeNode<T> n) {
    /*
     * Case 1: We deleted the root. All is well since we deleted one black node
     * from every path.
     */
    if (n.parent == null) {
      n.color = Color.BLACK;
      return;
    }

    /*
     * Case 2: We have a red sibling. Rotate about the parent to prepare for
     * remaining cases.
     */
    if (nodeColor(n.sibling()) == Color.RED) {
      n.parent.color = Color.RED;
      n.sibling().color = Color.BLACK;
      if (n == n.parent.left) {
        rotateLeft(n.parent);
      } else {
        rotateRight(n.parent);
      }
    }

    /*
     * Case 3: We can recolor and recurse
     */
    if (nodeColor(n.parent) == Color.BLACK
        && nodeColor(n.sibling()) == Color.BLACK
        && nodeColor(n.sibling().left) == Color.BLACK
        && nodeColor(n.sibling().right) == Color.BLACK) {
      n.sibling().color = Color.RED;
      fixTreeForDeletion(n.parent);
      return;
    }

    /*
     * Case 4
     */
    if (nodeColor(n.parent) == Color.RED
        && nodeColor(n.sibling()) == Color.BLACK
        && nodeColor(n.sibling().left) == Color.BLACK
        && nodeColor(n.sibling().right) == Color.BLACK) {
      n.sibling().color = Color.RED;
      n.parent.color = Color.BLACK;
      return;
    }

    /*
     * Case 5: Prepare for case 6
     */
    if (n == n.parent.left
        && nodeColor(n.sibling()) == Color.BLACK
        && nodeColor(n.sibling().left) == Color.RED
        && nodeColor(n.sibling().right) == Color.BLACK) {
      n.sibling().color = Color.RED;
      n.sibling().left.color = Color.BLACK;
      rotateRight(n.sibling());
    }
    else if (n == n.parent.right
        && nodeColor(n.sibling()) == Color.BLACK
        && nodeColor(n.sibling().right) == Color.RED
        && nodeColor(n.sibling().left) == Color.BLACK) {
      n.sibling().color = Color.RED;
      n.sibling().right.color = Color.BLACK;
      rotateLeft(n.sibling());
    }

    /*
     * Case 6
     */
    n.sibling().color = nodeColor(n.parent);
    n.parent.color = Color.BLACK;
    if (n == n.parent.left) {
      assert nodeColor(n.sibling().right) == Color.RED;
      n.sibling().right.color = Color.BLACK;
      rotateLeft(n.parent);
    }
    else {
      assert nodeColor(n.sibling().left) == Color.RED;
      n.sibling().left.color = Color.BLACK;
      rotateRight(n.parent);
    }
  }

  private TreeNode<T> maximumNode(TreeNode<T> n) {
    while (n.right != null) {
      n = n.right;
    }
    return n;
  }
}

/* License included from original source:
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 tinloaf
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
