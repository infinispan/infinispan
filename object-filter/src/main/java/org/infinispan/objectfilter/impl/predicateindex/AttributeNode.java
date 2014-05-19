package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An attribute node represents a single attribute and keeps track of subscribed predicates and projections.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public class AttributeNode<AttributeId extends Comparable<AttributeId>> {

   //todo [anistor] add here all metadata so it is precomputed and the MatcherEvalContext is simplified and more efficient

   // this is never null, except for the root node
   private final AttributeId attribute;

   public Object metadata;

   // this is never null, except for the root node
   private final AttributeNode<AttributeId> parent;

   /**
    * Child attributes.
    */
   private Map<AttributeId, AttributeNode<AttributeId>> children;

   /**
    * Root node must not have predicates. This field is always null for root node. Non-leaf nodes can only have
    * 'isEmpty' or 'isNull' predicates.
    */
   private Predicates predicates;

   /**
    * The list of all subscribed projections. This field is always null for root node. Only leaf nodes can have
    * projections.
    */
   private Projections projections;

   AttributeNode(AttributeId attribute, AttributeNode<AttributeId> parent) {
      this.attribute = attribute;
      this.parent = parent;
   }

   public AttributeId getAttribute() {
      return attribute;
   }

   public AttributeNode<AttributeId> getParent() {
      return parent;
   }

   public Iterator<AttributeNode<AttributeId>> getChildrenIterator() {
      return children != null ? children.values().iterator() : Collections.<AttributeNode<AttributeId>>emptyList().iterator();
   }

   /**
    * @param attribute
    * @return the child or null if not found
    */
   public AttributeNode<AttributeId> getChild(AttributeId attribute) {
      if (children != null) {
         return children.get(attribute);
      }
      return null;
   }

   public int getNumChildren() {
      return children != null ? children.size() : 0;
   }

   public boolean hasPredicates() {
      return predicates != null && !predicates.isEmpty();
   }

   public boolean hasProjections() {
      return projections != null && projections.hasProjections();
   }

   public void processValue(Object attributeValue, MatcherEvalContext<AttributeId> ctx) {
      if (projections != null) {
         projections.processProjections(ctx, attributeValue);
      }
      if (predicates != null) {
         predicates.notifyMatchingSubscribers(ctx, attributeValue);
      }
   }

   /**
    * Add a child node. If the child already exists it just increments its usage counter and returns the existing
    * child.
    *
    * @param attribute
    * @return the added or existing child
    */
   public AttributeNode<AttributeId> addChild(AttributeId attribute) {
      AttributeNode<AttributeId> child;
      if (children == null) {
         children = new HashMap<AttributeId, AttributeNode<AttributeId>>();
         child = new AttributeNode<AttributeId>(attribute, this);
         children.put(attribute, child);
      } else {
         child = children.get(attribute);
         if (child == null) {
            child = new AttributeNode<AttributeId>(attribute, this);
            children.put(attribute, child);
         }
      }
      return child;
   }

   /**
    * Decrement the usage counter of a child node and remove it if no usages remain. The removal works recursively up
    * the path to the root.
    *
    * @param attribute the attribute of the child to be removed
    */
   public void removeChild(AttributeId attribute) {
      if (children == null) {
         throw new IllegalArgumentException("No child found : " + attribute);
      }
      AttributeNode<AttributeId> child = children.get(attribute);
      if (child == null) {
         throw new IllegalArgumentException("No child found : " + attribute);
      }
      children.remove(attribute);
   }

   public void addPredicateSubscription(PredicateIndex.Subscription subscription) {
      if (predicates == null) {
         predicates = new Predicates();
      }
      predicates.addPredicate(subscription);
   }

   public void removePredicateSubscription(PredicateIndex.Subscription subscription) {
      if (predicates != null) {
         predicates.removePredicate(subscription);
      } else {
         throw new IllegalStateException("Reached illegal state");
      }
   }

   public void addProjection(FilterSubscriptionImpl filterSubscription, int position) {
      if (projections == null) {
         projections = new Projections();
      }
      projections.addProjection(filterSubscription, position);
   }

   public void removeProjections(FilterSubscriptionImpl filterSubscription) {
      if (projections != null) {
         projections.removeProjections(filterSubscription);
      } else {
         throw new IllegalStateException("Reached illegal state");
      }
   }

   @Override
   public String toString() {
      return "AttributeNode(" + attribute + ')';
   }
}
