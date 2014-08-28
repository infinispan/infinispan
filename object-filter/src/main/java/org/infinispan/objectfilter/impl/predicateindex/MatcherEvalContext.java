package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterRegistry;
import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;

import java.util.HashMap;
import java.util.Map;

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

   private FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> filterRegistry;

   /**
    * Current node during traversal of the attribute tree.
    */
   protected AttributeNode<AttributeMetadata, AttributeId> currentNode;

   private final Map<Predicate<?>, Counter> suspendedSubscriptionCounts = new HashMap<Predicate<?>, Counter>();

   private final Object instance;

   private FilterEvalContext singleFilterContext;

   /**
    * Each filter subscription has its own evaluation context, created on demand.
    */
   private FilterEvalContext[] filterContexts;

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

   public void initMultiFilterContext(FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> filterRegistry) {
      this.filterRegistry = filterRegistry;
      filterContexts = new FilterEvalContext[filterRegistry.getNumFilters()];
   }

   public FilterEvalContext initSingleFilterContext(FilterSubscriptionImpl filterSubscription) {
      singleFilterContext = new FilterEvalContext(this, filterSubscription);
      return singleFilterContext;
   }

   public boolean isSingleFilter() {
      return singleFilterContext != null;
   }

   public FilterEvalContext getFilterEvalContext(FilterSubscriptionImpl filterSubscription) {
      if (isSingleFilter()) {
         return singleFilterContext;
      }

      FilterEvalContext filterEvalContext = filterContexts[filterSubscription.index];
      if (filterEvalContext == null) {
         filterEvalContext = new FilterEvalContext(this, filterSubscription);
         filterContexts[filterSubscription.index] = filterEvalContext;
      }
      return filterEvalContext;
   }

   public void addSuspendedSubscription(Predicate<?> predicate) {
      if (isSingleFilter()) {
         return;
      }
      Counter counter = suspendedSubscriptionCounts.get(predicate);
      if (counter == null) {
         counter = new Counter();
         suspendedSubscriptionCounts.put(predicate, counter);
      }
      counter.value++;
   }

   public int getSuspendedSubscriptionsCounter(Predicate<AttributeId> predicate) {
      if (isSingleFilter()) {
         return -1;
      }

      Counter counter = suspendedSubscriptionCounts.get(predicate);
      return counter == null ? 0 : counter.value;
   }

   public void match() {
      filterRegistry.match(this);
   }

   public void process(AttributeNode<AttributeMetadata, AttributeId> node) {
      currentNode = node;
      processAttributes(currentNode, instance);
   }

   protected abstract void processAttributes(AttributeNode<AttributeMetadata, AttributeId> node, Object instance);

   private static final class Counter {
      int value;
   }
}
