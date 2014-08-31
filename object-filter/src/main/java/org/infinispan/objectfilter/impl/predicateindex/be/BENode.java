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
   protected int index;

   /**
    * The index of the last child.
    */
   protected int span;

   protected BENode(BENode parent) {
      this.parent = parent;
   }

   void setLocation(int index, int span) {
      this.index = index;
      this.span = span;
   }

   public final boolean isDecided(FilterEvalContext evalContext) {
      return evalContext.treeCounters[index] <= 0;
   }

   public abstract void handleChildValue(BENode child, boolean childValue, FilterEvalContext evalContext);

   public void suspendSubscription(FilterEvalContext evalContext) {
      // nothing to do
   }
}
