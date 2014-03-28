package org.infinispan.objectfilter.impl.predicateindex;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class RootNode<AttributeId extends Comparable<AttributeId>> extends AttributeNode<AttributeId> {

   RootNode() {
      super(null, null);
   }

   @Override
   void addPredicateSubscription(PredicateIndex.Subscription subscription) {
      throw new UnsupportedOperationException("Root node does not allow predicates");
   }

   @Override
   void removePredicateSubscription(PredicateIndex.Subscription subscription) {
      throw new UnsupportedOperationException("Root node does not allow predicates");
   }

   @Override
   public String toString() {
      return "RootNode";
   }
}
