package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterRegistry;
import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;

import java.util.HashMap;
import java.util.List;
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

   protected AttributeNode<AttributeMetadata, AttributeId> rootNode;

   /**
    * Current node during traversal of the attribute tree.
    */
   protected AttributeNode<AttributeMetadata, AttributeId> currentNode;

   private final Object userContext;

   private final Object instance;

   private final Object eventType;

   private FilterEvalContext singleFilterContext;

   /**
    * Each filter subscription has its own evaluation context, created on demand.
    */
   private FilterEvalContext[] filterContexts;

   private List<FilterSubscriptionImpl> filterSubscriptions;

   private Map<Predicate<?>, Counter> suspendedSubscriptionCounts;

   protected MatcherEvalContext(Object userContext, Object instance, Object eventType) {
      this.userContext = userContext;
      this.instance = instance;
      this.eventType = eventType;
   }

   public abstract TypeMetadata getEntityType();

   /**
    * The instance being matched with the filters.
    */
   public Object getInstance() {
      return instance;
   }

   public Object getUserContext() {
      return userContext;
   }

   public Object getEventType() {
      return eventType;
   }

   public void initMultiFilterContext(FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> filterRegistry) {
      rootNode = filterRegistry.getPredicateIndex().getRoot();
      suspendedSubscriptionCounts = new HashMap<Predicate<?>, Counter>();
      filterSubscriptions = filterRegistry.getFilterSubscriptions();
      filterContexts = new FilterEvalContext[filterSubscriptions.size()];
   }

   public FilterEvalContext initSingleFilterContext(FilterSubscriptionImpl filterSubscription) {
      singleFilterContext = new FilterEvalContext(this, filterSubscription);
      return singleFilterContext;
   }

   private boolean isSingleFilter() {
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

   public AttributeNode<AttributeMetadata, AttributeId> getRootNode() {
      return rootNode;
   }

   public void process(AttributeNode<AttributeMetadata, AttributeId> node) {
      currentNode = node;
      processAttributes(currentNode, instance);
      if (filterContexts != null) {
         notifySubscribers();
      }
   }

   private void notifySubscribers() {
      for (int i = 0; i < filterContexts.length; i++) {
         FilterEvalContext filterEvalContext = filterContexts[i];
         FilterSubscriptionImpl s = filterSubscriptions.get(i);
         if (filterEvalContext == null) {
            if (s.getBETree().getChildCounters()[0] == BETree.EXPR_TRUE) {
               filterEvalContext = new FilterEvalContext(this, s);
            } else {
               continue;
            }
         }
         if (filterEvalContext.isMatching()) {
            s.getCallback().onFilterResult(userContext, instance, eventType, filterEvalContext.getProjection(), filterEvalContext.getSortProjection());
         }
      }
   }

   protected abstract void processAttributes(AttributeNode<AttributeMetadata, AttributeId> node, Object instance);

   private static final class Counter {
      int value;
   }
}
