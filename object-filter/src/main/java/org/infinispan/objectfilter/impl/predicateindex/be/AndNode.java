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

      if (childValue) {
         if (--evalContext.treeCounters[index] == 0) {
            // value of this node has just been decided: TRUE
            if (parent != null) {
               // let the parent know
               return parent.handleChildValue(this, true, evalContext);
            } else {
               for (int i = index; i < span; i++) {
                  evalContext.beTree.getNodes()[i].suspendSubscription(evalContext);
               }
               return true;
            }
         } else {
            // value of this node cannot be decided yet, so we cannot tell the parent anything yet but let's at least mark down the children as 'satisfied'
            evalContext.treeCounters[child.index] = BETree.EXPR_TRUE;
            for (int i = child.index; i < child.span; i++) {
               evalContext.beTree.getNodes()[i].suspendSubscription(evalContext);
            }
            return false;
         }
      } else {
         // value of this node is decided: FALSE
         if (parent != null) {
            // let the parent know
            return parent.handleChildValue(this, false, evalContext);
         } else {
            evalContext.treeCounters[0] = BETree.EXPR_FALSE;
            for (int i = index; i < span; i++) {
               evalContext.beTree.getNodes()[i].suspendSubscription(evalContext);
            }
            return true;
         }
      }
   }
}
