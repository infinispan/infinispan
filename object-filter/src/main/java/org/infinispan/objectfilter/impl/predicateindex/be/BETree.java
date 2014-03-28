package org.infinispan.objectfilter.impl.predicateindex.be;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class BETree {

   /**
    * The tree is represented by the array of its nodes listed in pre-order, thus the first node is always the root. A
    * valid tree must be non-empty.
    */
   private final BENode[] nodes;

   /**
    * The number of direct children of each node.
    */
   private final int[] childCounters;

   public BETree(BENode[] nodes, int[] childCounters) {
      this.nodes = nodes;
      this.childCounters = childCounters;
   }

   public BENode[] getNodes() {
      return nodes;
   }

   public int[] getChildCounters() {
      return childCounters;
   }
}
