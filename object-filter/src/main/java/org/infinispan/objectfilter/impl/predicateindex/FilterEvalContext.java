package org.infinispan.objectfilter.impl.predicateindex;

import java.util.Arrays;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.predicateindex.be.BENode;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterEvalContext {

   private static final int[] FALSE_TREE = {BETree.EXPR_FALSE};

   public final BETree beTree;

   public final int[] treeCounters;

   public final MatcherEvalContext<?, ?, ?> matcherContext;

   private final Object[] projection;

   private final Comparable[] sortProjection;

   public FieldAccumulator[] acc;

   public FilterEvalContext(MatcherEvalContext<?, ?, ?> matcherContext, FilterSubscriptionImpl filterSubscription) {
      this.matcherContext = matcherContext;
      this.beTree = filterSubscription.getBETree();
      // check if event type is matching
      if (checkEventType(matcherContext.getEventType(), filterSubscription.getEventTypes())) {
         int[] childCounters = beTree.getChildCounters();
         this.treeCounters = Arrays.copyOf(childCounters, childCounters.length);
      } else {
         this.treeCounters = FALSE_TREE;
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
         if (eventType.equals(t)) {
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

   void processProjection(int position, Object value) {
      Object[] projection = this.projection;
      if (projection == null) {
         projection = this.sortProjection;
      } else {
         if (position >= projection.length) {
            position -= projection.length;
            projection = this.sortProjection;
         }
      }

      if (acc != null) {
         FieldAccumulator a = acc[position];
         if (a != null) {
            a.update(projection, value);
            return;
         }
      }

      // if this is a repeated attribute and no accumulator is used then keep the first occurrence in order to be consistent with the Lucene based implementation
      if (projection[position] == null) {
         projection[position] = value;
      }
   }
}
