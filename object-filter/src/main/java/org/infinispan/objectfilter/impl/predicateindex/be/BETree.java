package org.infinispan.objectfilter.impl.predicateindex.be;

/**
 * Boolean expression tree representation. A tree is immutable and could be shared by multiple filters.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class BETree {

   // Values 0 and -1 in the copy of childCounters used during evaluation are boolean, anything > 0 is undecided yet
   public static final int EXPR_TRUE = 0;
   public static final int EXPR_FALSE = -1;

   /**
    * The tree is represented by the array of its nodes listed in pre-order, thus the first node is always the root. A
    * valid tree must be non-empty.
    */
   private final BENode[] nodes;

   /**
    * The number of direct children of each node. This array is not mutated. A copy of it is made during evaluation.
    * When evaluating the tree the number of children is decremented every time a child is found to have a definite
    * value. If the counter becomes 0 then the node is considered satisfied (TRUE). If during evaluation we find a node
    * to be unsatisfied we mark it and all children with -1 (FALSE).
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
