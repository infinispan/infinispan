package org.infinispan.objectfilter.impl.predicateindex.be;

import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.IntervalPredicate;
import org.infinispan.objectfilter.impl.predicateindex.Predicate;

import java.util.List;

/**
 * A PredicateNode is a leaf node in a BETree that holds a Predicate instance. A PredicateNode instance is never reused
 * inside the same BETree or shared between multiple BETrees, but an entire BETree could be shared by multiple filters.
 * Multiple PredicateNodes could share the same Predicate instance.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class PredicateNode<AttributeId extends Comparable<AttributeId>> extends BENode {

   // the predicate can be shared by multiple PredicateNodes
   private final Predicate<?> predicate;

   /**
    * Indicates if the Predicate's condition is negated. This can be true only for condition predicates, never for
    * interval predicates.
    */
   private final boolean isNegated;

   private final List<AttributeId> attributePath;

   public PredicateNode(BENode parent, Predicate<?> predicate, boolean isNegated, List<AttributeId> attributePath) {
      super(parent);
      if (isNegated && predicate instanceof IntervalPredicate) {
         throw new IllegalArgumentException("Interval predicates should not be negated");
      }
      this.predicate = predicate;
      this.isNegated = isNegated;
      this.attributePath = attributePath;
   }

   public Predicate<?> getPredicate() {
      return predicate;
   }

   public boolean isNegated() {
      return isNegated;
   }

   public List<AttributeId> getAttributePath() {
      return attributePath;
   }

   @Override
   public void handleChildValue(BENode child, boolean childValue, FilterEvalContext evalContext) {
      assert child == null;

      final int value = childValue ? BETree.EXPR_TRUE : BETree.EXPR_FALSE;

      if (isDecided(evalContext)) {
         if (predicate.isRepeated() && evalContext.treeCounters[startIndex] == value) {
            // receiving the same value multiple times if fine if this is a repeated condition
            return;
         }
         throw new IllegalStateException("This should never be called again if the state of this node was previously decided.");
      }

      if (parent == null) {
         evalContext.treeCounters[startIndex] = value;
         suspendSubscription(evalContext);
      } else {
         parent.handleChildValue(this, childValue, evalContext);
      }
   }

   @Override
   public void suspendSubscription(FilterEvalContext ctx) {
      if (predicate.isRepeated()) {
         ctx.matcherContext.addSuspendedSubscription(predicate);
      }
   }

   @Override
   public String toString() {
      return "PredicateNode{" +
            "attributePath=" + attributePath +
            ", isNegated=" + isNegated +
            ", predicate=" + predicate +
            '}';
   }
}
