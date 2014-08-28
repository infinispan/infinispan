package org.infinispan.objectfilter.impl.predicateindex.be;

import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class OrNode extends BENode {

   public OrNode(BENode parent) {
      super(parent);
   }

   @Override
   public void handleChildValue(BENode child, boolean childValue, FilterEvalContext evalContext) {
      if (isDecided(evalContext)) {
         throw new IllegalStateException("This should never be called again because the state of this node has been decided already.");
      }

      if (childValue) {
         // value of this node is decided: TRUE
         if (parent != null) {
            // propagate to the parent, if we have a parent
            parent.handleChildValue(this, true, evalContext);
         } else {
            // mark this node as satisfied
            setState(BETree.EXPR_TRUE, evalContext);
         }
      } else {
         if (--evalContext.treeCounters[startIndex] == 0) {
            // value of this node is decided: FALSE
            if (parent != null) {
               // propagate to the parent, if we have a parent
               parent.handleChildValue(this, false, evalContext);
            } else {
               // mark this node as 'unsatisfied'
               setState(BETree.EXPR_FALSE, evalContext);
            }
         } else {
            // value of this node cannot be decided yet, so we cannot propagate to the parent anything yet but let's at least mark down the child as 'unsatisfied'
            child.setState(BETree.EXPR_FALSE, evalContext);
         }
      }
   }
}
