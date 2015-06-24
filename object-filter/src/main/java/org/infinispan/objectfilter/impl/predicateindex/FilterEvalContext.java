package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;
import org.infinispan.objectfilter.impl.predicateindex.be.BENode;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;

import java.util.Arrays;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterEvalContext {

   public final BETree beTree;

   public final int[] treeCounters;

   public final MatcherEvalContext<?, ?, ?> matcherContext;

   public final Object[] projection;

   public final Comparable[] sortProjection;

   public FilterEvalContext(MatcherEvalContext<?, ?, ?> matcherContext, FilterSubscriptionImpl filterSubscription) {
      this.matcherContext = matcherContext;
      this.beTree = filterSubscription.getBETree();
      if (checkEventType(matcherContext.getEventType(), filterSubscription.getEventTypes())) {
         int[] childCounters = beTree.getChildCounters();
         this.treeCounters = Arrays.copyOf(childCounters, childCounters.length);
      } else {
         this.treeCounters = new int[]{BETree.EXPR_FALSE};
         for (BENode node : beTree.getNodes()) {
            node.suspendSubscription(this);
         }
      }
      projection = filterSubscription.getProjection() != null ? new Object[filterSubscription.getProjection().length] : null;
      sortProjection = filterSubscription.getSortFields() != null ? new Comparable[filterSubscription.getSortFields().length] : null;
   }

   private boolean checkEventType(Object eventType, Object[] eventTypes) {
      if (eventTypes == null) {
         return true;
      }
      if (eventType == null) {
         return false;
      }
      for (Object t : eventTypes) {
         if (t.equals(eventType)) {
            return true;
         }
      }
      return false;
   }

   /**
    * Returns the result of the filter. This method should be called only after the evaluation of all predicates (except
    * the ones that were suspended).
    *
    * @return {@code true} if the filter matches the given input, {@code false} otherwise
    */
   public boolean isMatching() {
      return treeCounters[0] == BETree.EXPR_TRUE;
   }

   public Object[] getProjection() {
      return projection;
   }

   public Comparable[] getSortProjection() {
      return sortProjection;
   }
}
