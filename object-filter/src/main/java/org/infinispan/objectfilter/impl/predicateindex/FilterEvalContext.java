package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.predicateindex.be.BETree;

import java.util.Arrays;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterEvalContext {

   public final BETree beTree;

   // 0 true
   // -1 false
   // > 0 undecided
   public final int[] treeCounters;

   public final MatcherEvalContext<?> matcherContext;

   public FilterEvalContext(BETree beTree, MatcherEvalContext<?> matcherContext) {
      this.beTree = beTree;
      this.matcherContext = matcherContext;
      int[] childCounters = beTree.getChildCounters();
      this.treeCounters = Arrays.copyOf(childCounters, childCounters.length);
   }

   /**
    * Returns the result of the filter. This method should be called only after the evaluation of all predicates (except
    * the ones that were suspended).
    *
    * @return true if the filter matches the given input, false otherwise
    */
   public boolean getResult() {
      return treeCounters[0] == 0;
   }
}
