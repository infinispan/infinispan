package org.infinispan.objectfilter.impl.util;

import java.util.ArrayList;
import java.util.List;

/**
 * An Interval tree is an ordered tree data structure to hold Intervals. Specifically, it allows one to efficiently find
 * all Intervals that contain any given value in O(log n) time (see http://en.wikipedia.org/wiki/Interval_tree).
 * <p/>
 * The implementation is based on red-black trees (http://en.wikipedia.org/wiki/Red–black_tree). Additions and removals
 * are efficient and require only minimal rebalancing of the tree as opposed to other implementation approaches that
 * perform a full rebuild after insertion. Duplicate intervals are not stored but are coped for.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class IntervalTree<K extends Comparable<K>, V> {

   public static final class Node<K extends Comparable<K>, V> {

      /**
       * The interval. The low value is the key of this node within the search tree.
       */
      public final Interval<K> interval;    //todo maybe it's wise to make it private and expose getter

      /**
       * A user payload value.
       */
      public V value;                       //todo maybe it's wise to make it private and expose getter and setter

      /**
       * The maximum value of any Interval endpoint stored in the subtree rooted at this node.
       */
      private K max;

      /**
       * The parent node.
       */
      private Node<K, V> parent;

      /**
       * The left child.
       */
      private Node<K, V> left;

      /**
       * The right child.
       */
      private Node<K, V> right;

      /**
       * Indicates the color of this node (either red or black).
       */
      private boolean isRed = false;

      private Node(Interval<K> interval) {
         this.interval = interval;
         this.max = interval.up;
      }

      private Node() {
         interval = null;
      }
   }

   private final Node<K, V> sentinel;

   /**
    * The root of the tree.
    */
   private Node<K, V> root;

   public IntervalTree() {
      sentinel = new Node<K, V>();
      sentinel.left = sentinel;
      sentinel.right = sentinel;
      sentinel.parent = sentinel;
      root = sentinel;
   }

   private int compare(K k1, K k2) {
      if (k1 == Interval.getMinusInf() || k2 == Interval.getPlusInf()) return -1;
      if (k1 == Interval.getPlusInf() || k2 == Interval.getMinusInf()) return 1;
      return k1.compareTo(k2);
   }

   private K max(K k1, K k2) {
      return compare(k1, k2) >= 0 ? k1 : k2;
   }

   private boolean compareLowerBound(Interval<K> i1, Interval<K> i2) {
      int res = compare(i1.low, i2.low);
      return res < 0 || res == 0 && (i1.includeLower || !i2.includeUpper);
   }

   /**
    * Compare two Intervals.
    *
    * @return a negative integer, zero, or a positive integer depending if Interval i1 is to the left of i2, overlaps
    * with it, or is to the right of i2.
    */
   private int compareIntervals(Interval<K> i1, Interval<K> i2) {
      int res1 = compare(i1.up, i2.low);
      if (res1 < 0 || res1 <= 0 && (!i1.includeUpper || !i2.includeLower)) {
         return -1;
      }
      int res2 = compare(i2.up, i1.low);
      if (res2 < 0 || res2 <= 0 && (!i2.includeUpper || !i1.includeLower)) {
         return 1;
      }
      return 0;
   }

   private void checkValidInterval(Interval<K> interval) {
      if (interval == null) {
         throw new IllegalArgumentException("Interval cannot be null");
      }
      if (compare(interval.low, interval.up) > 0) {
         throw new IllegalArgumentException("Interval lower bound cannot be higher than the upper bound");
      }
   }

   /**
    * Add the {@code Interval} into this {@code IntervalTree} and return the Node. Possible duplicates are found and the
    * existing Node is returned instead of adding a new one.
    *
    * @param i an Interval to be inserted
    */
   public Node<K, V> add(Interval<K> i) {
      checkValidInterval(i);
      return add(new Node<K, V>(i));
   }

   private Node<K, V> add(Node<K, V> n) {
      n.left = n.right = sentinel;

      Node<K, V> y = root;
      Node<K, V> x = root != null ? root.left : null;
      while (x != sentinel) {
         y = x;
         if (x.interval.equals(n.interval)) {
            return x;
         }
         if (compareLowerBound(n.interval, y.interval)) {
            x = x.left;
         } else {
            x = x.right;
         }
         y.max = max(n.max, y.max);
         if (y.parent == root) {
            root.max = y.max;
         }
      }
      n.parent = y;
      if (root != null && y == root) {
         root.max = n.max;
      }
      if (y != null) {
         if (y == root || compareLowerBound(n.interval, y.interval)) {
            y.left = n;
         } else {
            y.right = n;
         }
      }
      rebalanceAfterAdd(n);
      return n;
   }

   private void rebalanceAfterAdd(Node<K, V> z) {
      z.isRed = true;
      while (z.parent.isRed) {
         if (z.parent == z.parent.parent.left) {
            Node<K, V> y = z.parent.parent.right;
            if (y.isRed) {
               z.parent.isRed = false;
               y.isRed = false;
               z.parent.parent.isRed = true;
               z = z.parent.parent;
            } else {
               if (z == z.parent.right) {
                  z = z.parent;
                  rotateLeft(z);
               }
               z.parent.isRed = false;
               z.parent.parent.isRed = true;
               rotateRight(z.parent.parent);
            }
         } else {
            Node<K, V> y = z.parent.parent.left;
            if (y.isRed) {
               z.parent.isRed = false;
               y.isRed = false;
               z.parent.parent.isRed = true;
               z = z.parent.parent;
            } else {
               if (z == z.parent.left) {
                  z = z.parent;
                  rotateRight(z);
               }
               z.parent.isRed = false;
               z.parent.parent.isRed = true;
               rotateLeft(z.parent.parent);
            }
         }
      }
      root.left.isRed = false;
   }

   private void rotateLeft(Node<K, V> x) {
      Node<K, V> y = x.right;

      x.right = y.left;
      if (y.left != sentinel)
         y.left.parent = x;
      y.parent = x.parent;
      if (x == x.parent.left) {
         x.parent.left = y;
      } else {
         x.parent.right = y;
      }
      y.left = x;
      x.parent = y;

      if (y.parent == root) {
         root.max = x.max;
      }
      y.max = x.max;
      x.max = max(x.interval.up, max(x.left.max, x.right.max));
   }

   private void rotateRight(Node<K, V> x) {
      Node<K, V> y = x.left;

      x.left = y.right;
      if (y.right != sentinel) {
         y.right.parent = x;
      }
      y.parent = x.parent;
      if (x == x.parent.left) {
         x.parent.left = y;
      } else {
         x.parent.right = y;
      }
      y.right = x;
      x.parent = y;

      if (y.parent == root) {
         root.max = x.max;
      }
      y.max = x.max;
      x.max = max(x.interval.up, max(x.left.max, x.right.max));
   }

   /**
    * Removes the Interval.
    *
    * @param i the interval to remove
    */
   public boolean remove(Interval<K> i) {
      checkValidInterval(i);
      return remove(root.left, i);
   }

   private boolean remove(Node<K, V> n, Interval<K> i) {
      if (n == sentinel || compare(i.low, n.max) > 0) {
         return false;
      }

      if (n.interval.equals(i)) {
         remove(n);
         return true;
      }

      if (n.left != sentinel && remove(n.left, i)) {
         return true;
      }

      if (compareIntervals(i, n.interval) < 0) {
         return false;
      }

      return n.right != sentinel && remove(n.right, i);
   }

   public void remove(Node<K, V> n) {
      n.max = Interval.<K>getMinusInf();
      for (Node<K, V> i = n.parent; i != root; i = i.parent) {
         i.max = max(i.left.max, i.right.max);
         if (i.parent == root) {
            root.max = i.max;
         }
      }

      Node<K, V> y;
      Node<K, V> x;

      if (n.left == sentinel || n.right == sentinel) {
         y = n;
      } else {
         y = findSuccessor(n);
      }

      if (y.left == sentinel) {
         x = y.right;
      } else {
         x = y.left;
      }

      x.parent = y.parent;
      if (root == x.parent) {
         root.left = x;
      } else if (y == y.parent.left) {
         y.parent.left = x;
      } else {
         y.parent.right = x;
      }
      if (y != n) {
         if (!y.isRed) {
            rebalanceAfterRemove(x);
         }

         y.left = n.left;
         y.right = n.right;
         y.parent = n.parent;
         y.isRed = n.isRed;
         n.left.parent = n.right.parent = y;
         if (n == n.parent.left) {
            n.parent.left = y;
         } else {
            n.parent.right = y;
         }
      } else if (!y.isRed) {
         rebalanceAfterRemove(x);
      }
   }

   private Node<K, V> findSuccessor(Node<K, V> x) {
      Node<K, V> successor = x.right;
      if (successor != sentinel) {
         while (successor.left != sentinel) {
            successor = successor.left;
         }
         return successor;
      }
      successor = x.parent;
      while (x == successor.right) {
         x = successor;
         successor = successor.parent;
      }
      if (successor == root) {
         return sentinel;
      }
      return successor;
   }

   private void rebalanceAfterRemove(Node<K, V> x) {
      while (x != root.left && !x.isRed) {
         if (x == x.parent.left) {
            Node<K, V> w = x.parent.right;
            if (w.isRed) {
               w.isRed = false;
               x.parent.isRed = true;
               rotateLeft(x.parent);
               w = x.parent.right;
            }
            if (!w.left.isRed && !w.right.isRed) {
               w.isRed = true;
               x = x.parent;
            } else {
               if (!w.right.isRed) {
                  w.left.isRed = false;
                  w.isRed = true;
                  rotateRight(w);
                  w = x.parent.right;
               }
               w.isRed = x.parent.isRed;
               x.parent.isRed = false;
               w.right.isRed = false;
               rotateLeft(x.parent);
               x = root.left;
            }
         } else {
            Node<K, V> w = x.parent.left;
            if (w.isRed) {
               w.isRed = false;
               x.parent.isRed = true;
               rotateRight(x.parent);
               w = x.parent.left;
            }
            if (!w.right.isRed && !w.left.isRed) {
               w.isRed = true;
               x = x.parent;
            } else {
               if (!w.left.isRed) {
                  w.right.isRed = false;
                  w.isRed = true;
                  rotateLeft(w);
                  w = x.parent.left;
               }
               w.isRed = x.parent.isRed;
               x.parent.isRed = false;
               w.left.isRed = false;
               rotateRight(x.parent);
               x = root.left;
            }
         }
      }
      x.isRed = false;
   }

   /**
    * Checks if this {@code IntervalTree} does not have any Intervals.
    *
    * @return {@code true} if this {@code IntervalTree} is empty, {@code false} otherwise.
    */
   public boolean isEmpty() {
      return root.left == sentinel;
   }

   /**
    * Find all Intervals that contain a given value.
    *
    * @param k the value to search for
    * @return a non-null List of intervals that contain the value
    */
   public List<Node<K, V>> stab(K k) {
      Interval<K> i = new Interval<K>(k, true, k, true);
      final List<Node<K, V>> nodes = new ArrayList<Node<K, V>>();
      findOverlap(root.left, i, new NodeCallback<K, V>() {
         @Override
         public void handle(Node<K, V> node) {
            nodes.add(node);
         }
      });
      return nodes;
   }

   public void stab(K k, NodeCallback<K, V> nodeCallback) {
      Interval<K> i = new Interval<K>(k, true, k, true);
      findOverlap(root.left, i, nodeCallback);
   }

   private void findOverlap(Node<K, V> n, Interval<K> i, NodeCallback<K, V> nodeCallback) {
      if (n == sentinel || compare(i.low, n.max) > 0) {
         return;
      }

      if (n.left != sentinel) {
         findOverlap(n.left, i, nodeCallback);
      }

      if (compareIntervals(n.interval, i) == 0) {
         nodeCallback.handle(n);
      }

      if (compareIntervals(i, n.interval) < 0) {
         return;
      }

      if (n.right != sentinel) {
         findOverlap(n.right, i, nodeCallback);
      }
   }

   public Node<K, V> findNode(Interval<K> i) {
      checkValidInterval(i);
      return findNode(root.left, i);
   }

   private Node<K, V> findNode(Node<K, V> n, Interval<K> i) {
      if (n == sentinel || compare(i.low, n.max) > 0) {
         return null;
      }

      if (n.interval.equals(i)) {
         return n;
      }

      if (n.left != sentinel) {
         Node<K, V> w = findNode(n.left, i);
         if (w != null) {
            return w;
         }
      }

      if (compareIntervals(i, n.interval) < 0) {
         return null;
      }

      if (n.right != sentinel) {
         return findNode(n.right, i);
      }

      return null;
   }

   public interface NodeCallback<K extends Comparable<K>, V> {
      void handle(Node<K, V> node);
   }

   public void inorderTraversal(NodeCallback<K, V> nodeCallback) {
      inorderTraversal(root.left, nodeCallback);
   }

   private void inorderTraversal(Node<K, V> n, NodeCallback<K, V> nodeCallback) {
      if (n != sentinel) {
         inorderTraversal(n.left, nodeCallback);
         nodeCallback.handle(n);
         inorderTraversal(n.right, nodeCallback);
      }
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder();
      inorderTraversal(new NodeCallback<K, V>() {
         @Override
         public void handle(Node<K, V> n) {
            if (sb.length() > 0) {
               sb.append(", ");
            }
            sb.append(n.interval);
            sb.append("->{");
            sb.append(n.value);
            sb.append('}');
         }
      });
      return sb.toString();
   }
}
