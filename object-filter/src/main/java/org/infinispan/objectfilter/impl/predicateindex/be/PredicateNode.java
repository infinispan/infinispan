package org.infinispan.objectfilter.impl.predicateindex.be;

import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.Predicate;
import org.infinispan.objectfilter.impl.predicateindex.PredicateIndex;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class PredicateNode<AttributeId extends Comparable<AttributeId>> extends BENode {

   private final Predicate predicate;

   /**
    * Indicates if the predicate's condition is negated. This can be true only for condition predicates, never for
    * interval predicates.
    */
   private final boolean isNegated;

   /**
    * Indicates if the predicate is evaluated multiple times because one of the path components is a collection/array.
    */
   private final boolean isRepeated;

   private final List<AttributeId> attributePath;

   private PredicateIndex.Subscription subscription;

   public PredicateNode(BENode parent, Predicate predicate, boolean isNegated, List<AttributeId> attributePath, boolean isRepeated) {
      super(parent);
      if (isNegated && predicate.getInterval() != null) {
         throw new IllegalArgumentException("Interval predicates should not be negated");
      }
      this.predicate = predicate;
      this.isNegated = isNegated;
      this.attributePath = attributePath;
      this.isRepeated = isRepeated;
   }

   public Predicate getPredicate() {
      return predicate;
   }

   public boolean isNegated() {
      return isNegated;
   }

   public List<AttributeId> getAttributePath() {
      return attributePath;
   }

   public boolean isRepeated() {
      return isRepeated;
   }

   @Override
   public boolean handleChildValue(BENode child, boolean childValue, FilterEvalContext evalContext) {
      if (child != null) {
         throw new IllegalArgumentException();
      }

      final int value = childValue ? BETree.EXPR_TRUE : BETree.EXPR_FALSE;

      if (evalContext.treeCounters[index] <= 0) {
         if (isRepeated && evalContext.treeCounters[index] == value) {
            // receiving the same value multiple times if fine if this is a repeated condition
            // here we return a status that is most likely incorrect but it is harmless
            return false;
         }
         throw new IllegalStateException("This should never be called again if the state of this node was previously decided.");
      }

      evalContext.treeCounters[index] = value;

      if (parent == null) {
         suspendSubscription(evalContext);
         return true;
      }

      return parent.handleChildValue(this, childValue, evalContext);
   }

   public void subscribe(PredicateIndex<AttributeId> predicateIndex, Predicate.Callback callback) {
      if (subscription != null) {
         throw new IllegalStateException("Already subscribed");
      }
      subscription = predicateIndex.addSubscriptionForPredicate(this, callback);
   }

   public void unsubscribe() {
      subscription.cancel();
      subscription = null;
   }

   @Override
   public void suspendSubscription(FilterEvalContext ctx) {
      subscription.suspend(ctx.matcherContext);
   }

   @Override
   public String toString() {
      return "PredicateNode{" +
            "attributePath=" + attributePath +
            ", predicate=" + predicate +
            ", isNegated=" + isNegated +
            ", isRepeated=" + isRepeated +
            '}';
   }
}
