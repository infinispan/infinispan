package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;
import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class RootNode<AttributeMetadata, AttributeId extends Comparable<AttributeId>> extends AttributeNode<AttributeMetadata, AttributeId> {

   RootNode(MetadataAdapter<?, AttributeMetadata, AttributeId> metadataAdapter) {
      super(metadataAdapter);
   }

   @Override
   public Predicates.Subscription<AttributeId> addPredicateSubscription(PredicateNode<AttributeId> predicateNode, FilterSubscriptionImpl filterSubscription) {
      throw new UnsupportedOperationException("Root node does not allow predicates");
   }

   @Override
   public void removePredicateSubscription(Predicates.Subscription<AttributeId> subscription) {
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
