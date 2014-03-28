package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps track of all predicates from all filters of an entity type and determines efficiently which predicates match a
 * given entity instance. There is a single instance at most of this for class per each entity type. The predicates are
 * stored in an index-like structure to allow fast matching and are reference counted in order to allow sharing of
 * predicates between filters rather than duplicating them.
 *
 * @param <AttributeId> is the type used to represent attribute IDs (usually String or Integer)
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class PredicateIndex<AttributeId extends Comparable<AttributeId>> {

   public class Subscription {
      private final PredicateNode<AttributeId> predicateNode;
      private final Predicate.Callback callback;
      private boolean isSuspended = false;

      Subscription(PredicateNode<AttributeId> predicateNode, Predicate.Callback callback) {
         this.predicateNode = predicateNode;
         this.callback = callback;
      }

      public Predicate getPredicate() {
         return predicateNode.getPredicate();
      }

      public void suspend(MatcherEvalContext<AttributeId> ctx) {
         if (!isSuspended) {
            isSuspended = true;
            AtomicInteger suspendedCounter = ctx.getSuspendedSubscriptionsCounter(predicateNode.getPredicate());
            suspendedCounter.incrementAndGet();
         }
      }

      public void cancel() {
         AttributeNode<AttributeId> current = root;
         for (AttributeId attribute : predicateNode.getAttributePath()) {
            current = current.getChild(attribute);
            if (current == null) {
               throw new IllegalStateException("Child not found : " + attribute);
            }
         }
         current.removePredicateSubscription(this);

         // remove the nodes that no longer have a purpose
         while (current != root) {
            if (current.getNumChildren() > 0 || current.hasPredicates() || current.hasProjections()) {
               return;
            }

            AttributeId childId = current.getAttribute();
            current = current.getParent();
            current.removeChild(childId);
         }
      }

      public void handleValue(MatcherEvalContext<?> ctx, boolean isMatching) {
         if (!isSuspended) {
            if (predicateNode.isNegated()) {
               isMatching = !isMatching;
            }
            if (isMatching || !predicateNode.isRepeated()) {
               callback.handleValue(ctx, isMatching);
            }
         }
      }
   }

   private final AttributeNode<AttributeId> root = new RootNode<AttributeId>();

   public AttributeNode<AttributeId> getRoot() {
      return root;
   }

   public Subscription addSubscriptionForPredicate(PredicateNode<AttributeId> predicateNode, Predicate.Callback callback) {
      AttributeNode<AttributeId> currentNode = root;
      for (AttributeId attribute : predicateNode.getAttributePath()) {
         currentNode = currentNode.addChild(attribute);
      }

      Subscription subscription = new Subscription(predicateNode, callback);
      currentNode.addPredicateSubscription(subscription);
      return subscription;
   }
}
