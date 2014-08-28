package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;
import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;

import java.util.List;

/**
 * Keeps track of all predicates and all projections from all filters of an entity type and determines efficiently which
 * predicates match a given entity instance. There is a single instance at most of this for class per each entity type.
 * The predicates are stored in an index-like structure to allow fast matching and are reference counted in order to
 * allow sharing of predicates between filters rather than duplicating them.
 *
 * @param <AttributeMetadata> is the type of the metadata attached to an AttributeNode
 * @param <AttributeId>       is the type used to represent attribute IDs (usually String or Integer)
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class PredicateIndex<AttributeMetadata, AttributeId extends Comparable<AttributeId>> {

   private final AttributeNode<AttributeMetadata, AttributeId> root;

   public PredicateIndex(MetadataAdapter<?, AttributeMetadata, AttributeId> metadataAdapter) {
      root = new RootNode<AttributeMetadata, AttributeId>(metadataAdapter);
   }

   public AttributeNode<AttributeMetadata, AttributeId> getRoot() {
      return root;
   }

   public Predicates.Subscription<AttributeId> addSubscriptionForPredicate(PredicateNode<AttributeId> predicateNode, FilterSubscriptionImpl filterSubscription) {
      AttributeNode<AttributeMetadata, AttributeId> attributeNode = addAttributeNodeByPath(predicateNode.getAttributePath());
      predicateNode.getPredicate().attributeNode = attributeNode;
      return attributeNode.addPredicateSubscription(predicateNode, filterSubscription);
   }

   public void removeSubscriptionForPredicate(Predicates.Subscription<AttributeId> subscription) {
      AttributeNode<AttributeMetadata, AttributeId> current = getAttributeNodeByPath(subscription.getPredicateNode().getAttributePath());
      current.removePredicateSubscription(subscription);

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

   private AttributeNode<AttributeMetadata, AttributeId> getAttributeNodeByPath(List<AttributeId> attributePath) {
      AttributeNode<AttributeMetadata, AttributeId> node = root;
      for (AttributeId attribute : attributePath) {
         node = node.getChild(attribute);
         if (node == null) {
            throw new IllegalStateException("Child not found : " + attribute);
         }
      }
      return node;
   }

   private AttributeNode<AttributeMetadata, AttributeId> addAttributeNodeByPath(List<AttributeId> attributePath) {
      AttributeNode<AttributeMetadata, AttributeId> node = root;
      for (AttributeId attribute : attributePath) {
         node = node.addChild(attribute);
      }
      return node;
   }

   public int addProjections(FilterSubscriptionImpl<?, AttributeMetadata, AttributeId> filterSubscription, List<List<AttributeId>> projection, int i) {
      for (List<AttributeId> projectionPath : projection) {
         AttributeNode node = addAttributeNodeByPath(projectionPath);
         node.addProjection(filterSubscription, i++);
      }
      return i;
   }

   public void removeProjections(FilterSubscriptionImpl<?, AttributeMetadata, AttributeId> filterSubscription, List<List<AttributeId>> projection) {
      for (List<AttributeId> projectionPath : projection) {
         AttributeNode node = getAttributeNodeByPath(projectionPath);
         node.removeProjections(filterSubscription);
      }
   }
}
