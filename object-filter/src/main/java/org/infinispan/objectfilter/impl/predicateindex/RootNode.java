package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class RootNode<AttributeId extends Comparable<AttributeId>> extends AttributeNode<AttributeId> {

   RootNode() {
      super(null, null);
   }

   @Override
   public void addPredicateSubscription(PredicateIndex.Subscription subscription) {
      throw new UnsupportedOperationException("Root node does not allow predicates");
   }

   @Override
   public void removePredicateSubscription(PredicateIndex.Subscription subscription) {
      throw new UnsupportedOperationException("Root node does not allow predicates");
   }

   @Override
   public void addProjection(FilterSubscriptionImpl filterSubscription, int position) {
      throw new UnsupportedOperationException("Root node does not allow projections");
   }

   @Override
   public void removeProjections(FilterSubscriptionImpl filterSubscription) {
      throw new UnsupportedOperationException("Root node does not allow projections");
   }

   @Override
   public String toString() {
      return "RootNode";
   }
}
