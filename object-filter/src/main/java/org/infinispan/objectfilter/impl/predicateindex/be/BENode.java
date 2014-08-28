package org.infinispan.objectfilter.impl.predicateindex.be;

import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;

/**
 * Base boolean expression Node.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class BENode {

   /**
    * The parent node or null if this is the root;
    */
   protected final BENode parent;

   /**
    * The index of this node in the tree's node array.
    */
   protected int startIndex;

   /**
    * The index of the last child.
    */
   protected int endIndex;

   protected BENode(BENode parent) {
      this.parent = parent;
   }

   void setLocation(int startIndex, int endIndex) {
      this.startIndex = startIndex;
      this.endIndex = endIndex;
   }

   public final boolean isDecided(FilterEvalContext evalContext) {
      return evalContext.treeCounters[0] <= 0 || evalContext.treeCounters[startIndex] <= 0;
   }

   public abstract void handleChildValue(BENode child, boolean childValue, FilterEvalContext evalContext);

   public void suspendSubscription(FilterEvalContext evalContext) {
      // nothing to do here, subclasses must override appropriately
   }

   protected final void setState(int nodeValue, FilterEvalContext evalContext) {
      BENode[] nodes = evalContext.beTree.getNodes();
      for (int i = startIndex; i < endIndex; i++) {
         if (evalContext.treeCounters[i] == 1) {
            // this may be a predicate node
            evalContext.treeCounters[i] = nodeValue; // this is not the real value, but any value less that 1 will do
            nodes[i].suspendSubscription(evalContext);
         }
      }
      evalContext.treeCounters[startIndex] = nodeValue;
   }
}
