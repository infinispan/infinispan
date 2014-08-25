package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;
import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stores processing state during the matching process of all filters registered with a Matcher.
 *
 * @param <TypeMetadata>      representation of entity type information, ie a Class object or anything that represents a
 *                            type
 * @param <AttributeMetadata> representation of attribute type information
 * @param <AttributeId>       representation of attribute identifiers
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> {

   /**
    * Current node during traversal of the attribute tree.
    */
   protected AttributeNode<AttributeMetadata, AttributeId> currentNode;

   /**
    * Each filter subscription has its own evaluation context, created on demand.
    */
   private final Map<FilterSubscriptionImpl, FilterEvalContext> filterContexts = new HashMap<FilterSubscriptionImpl, FilterEvalContext>();

   private final Map<Predicate<?>, AtomicInteger> suspendedSubscriptionCounts = new HashMap<Predicate<?>, AtomicInteger>();

   private final Set<PredicateNode<AttributeId>> suspendedSubscriptions = new HashSet<PredicateNode<AttributeId>>();

   private final Object instance;

   private FilterEvalContext singleFilterContext;

   protected MatcherEvalContext(Object instance) {
      this.instance = instance;
   }

   public abstract TypeMetadata getEntityType();

   /**
    * The instance being matched with the filters.
    */
   public Object getInstance() {
      return instance;
   }

   public FilterEvalContext initSingleFilterContext(FilterSubscriptionImpl filterSubscription) {
      singleFilterContext = new FilterEvalContext(this, filterSubscription);
      return singleFilterContext;
   }

   public FilterEvalContext getFilterEvalContext(FilterSubscriptionImpl filterSubscription) {
      if (singleFilterContext != null) {
         return singleFilterContext;
      }

      FilterEvalContext filterEvalContext = filterContexts.get(filterSubscription);
      if (filterEvalContext == null) {
         filterEvalContext = new FilterEvalContext(this, filterSubscription);
         filterContexts.put(filterSubscription, filterEvalContext);
      }
      return filterEvalContext;
   }

   public void addSuspendedSubscription(PredicateNode<AttributeId> predicateNode) {
      suspendedSubscriptions.add(predicateNode);
      AtomicInteger counter = suspendedSubscriptionCounts.get(predicateNode.getPredicate());
      if (counter == null) {
         counter = new AtomicInteger();
         suspendedSubscriptionCounts.put(predicateNode.getPredicate(), counter);
      }
      counter.incrementAndGet();
   }

   public boolean isSuspendedSubscription(PredicateNode<AttributeId> predicateNode) {
      return suspendedSubscriptions.contains(predicateNode);
   }

   public int getSuspendedSubscriptionsCounter(Predicate<AttributeId> predicate) {
      AtomicInteger counter = suspendedSubscriptionCounts.get(predicate);
      return counter == null ? 0 : counter.get();
   }

   public void process(AttributeNode<AttributeMetadata, AttributeId> node) {
      currentNode = node;
      processAttributes(currentNode, instance);
   }

   protected abstract void processAttributes(AttributeNode<AttributeMetadata, AttributeId> node, Object instance);
}
