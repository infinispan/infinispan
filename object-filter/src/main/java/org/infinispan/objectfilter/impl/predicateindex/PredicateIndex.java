package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;

import java.util.List;

/**
 * Keeps track of all predicates and all projections from all filters of an entity type and determines efficiently which
 * predicates match a given entity instance. There is a single instance at most of this for class per each entity type.
 * The predicates are stored in an index-like structure to allow fast matching and are reference counted in order to
 * allow sharing of predicates between filters rather than duplicating them.
 *
 * @param <AttributeId> is the type used to represent attribute IDs (usually String or Integer)
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class PredicateIndex<AttributeId extends Comparable<AttributeId>> {

   public final class Subscription {
      private final PredicateNode<AttributeId> predicateNode;
      private final Predicate.Callback callback;

      Subscription(PredicateNode<AttributeId> predicateNode, Predicate.Callback callback) {
         this.predicateNode = predicateNode;
         this.callback = callback;
      }

      public Predicate getPredicate() {
         return predicateNode.getPredicate();
      }

      public void suspend(MatcherEvalContext<AttributeId> ctx) {
         ctx.addSuspendedSubscription(predicateNode);
      }

      public void cancel() {
         AttributeNode<AttributeId> current = getAttributeNodeByPath(predicateNode.getAttributePath());
         current.removePredicateSubscription(this);

         // remove the nodes that no longer have a purpose
         while (current != root) {
            if (current.getNumChildren() > 0 || current.hasPredicates() || current.hasProjections()) {
               break;
            }

            AttributeId childId = current.getAttribute();
            current = current.getParent();
            current.removeChild(childId);
         }
      }

      public void handleValue(MatcherEvalContext<AttributeId> ctx, boolean isMatching) {
         if (!ctx.isSuspendedSubscription(predicateNode)) {
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
      AttributeNode<AttributeId> attributeNode = addAttributeNodeByPath(predicateNode.getAttributePath());
      Subscription subscription = new Subscription(predicateNode, callback);
      attributeNode.addPredicateSubscription(subscription);
      return subscription;
   }

   public AttributeNode<AttributeId> getAttributeNodeByPath(List<AttributeId> attributePath) {
      AttributeNode<AttributeId> node = root;
      for (AttributeId attribute : attributePath) {
         node = node.getChild(attribute);
         if (node == null) {
            throw new IllegalStateException("Child not found : " + attribute);
         }
      }
      return node;
   }

   public AttributeNode<AttributeId> addAttributeNodeByPath(List<AttributeId> attributePath) {
      AttributeNode<AttributeId> node = root;
      for (AttributeId attribute : attributePath) {
         node = node.addChild(attribute);
      }
      return node;
   }
}
