package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;
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
      int[] childCounters = beTree.getChildCounters();
      this.treeCounters = Arrays.copyOf(childCounters, childCounters.length);
      projection = filterSubscription.getProjection() != null ? new Object[filterSubscription.getProjection().length] : null;
      sortProjection = filterSubscription.getSortFields() != null ? new Comparable[filterSubscription.getSortFields().length] : null;
   }

   /**
    * Returns the result of the filter. This method should be called only after the evaluation of all predicates (except
    * the ones that were suspended).
    *
    * @return true if the filter matches the given input, false otherwise
    */
   public boolean getMatchResult() {
      return treeCounters[0] == BETree.EXPR_TRUE;
   }

   public Object[] getProjection() {
      return projection;
   }

   public Comparable[] getSortProjection() {
      return sortProjection;
   }
}
