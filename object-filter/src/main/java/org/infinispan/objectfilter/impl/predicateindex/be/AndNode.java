package org.infinispan.objectfilter.impl.predicateindex.be;

import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class AndNode extends BENode {

   public AndNode(BENode parent) {
      super(parent);
   }

   @Override
   public boolean handleChildValue(BENode child, boolean childValue, FilterEvalContext evalContext) {
      if (evalContext.treeCounters[index] <= 0) {
         throw new IllegalStateException("This should never be called again because the state of this node has been decided already.");
      }

      if (parent == null) {
         if (childValue) {
            if (--evalContext.treeCounters[index] == BETree.EXPR_TRUE) {
               for (int i = index; i < span; i++) {
                  evalContext.beTree.getNodes()[i].suspendSubscription(evalContext);
               }
               return true;
            }
            return false;
         } else {
            evalContext.treeCounters[0] = BETree.EXPR_FALSE;
            for (int i = index; i < span; i++) {
               evalContext.beTree.getNodes()[i].suspendSubscription(evalContext);
            }
            return true;
         }
      }

      if (childValue) {
         if (--evalContext.treeCounters[index] == BETree.EXPR_TRUE) {
            // value of this node has just been decided, let the parent know
            return parent.handleChildValue(this, true, evalContext);
         } else {
            // value of this node cannot be decided yet, so we cannot tell the parent anything yet but let's at least mark down the children as 'satisfied'
            for (int i = child.index; i < child.span; i++) {
               evalContext.treeCounters[i] = BETree.EXPR_TRUE;
               evalContext.beTree.getNodes()[i].suspendSubscription(evalContext);
            }
            return false;
         }
      } else {
         // value of this node is decided, let the parent know
         return parent.handleChildValue(this, false, evalContext);
      }
   }
}
