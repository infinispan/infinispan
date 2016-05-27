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

   private Map<Predicate<?>, Counter> suspendedPredicateSubscriptionCounts;

   protected MatcherEvalContext(Object userContext, Object eventType, Object instance) {
      if (instance == null) {
         throw new IllegalArgumentException("instance cannot be null");
      }
      this.userContext = userContext;
      this.eventType = eventType;
      this.instance = instance;
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
      suspendedPredicateSubscriptionCounts = new HashMap<>();
      filterSubscriptions = filterRegistry.getFilterSubscriptions();
      filterContexts = new FilterEvalContext[filterRegistry.getFilterSubscriptions().size()];
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
      Counter counter = suspendedPredicateSubscriptionCounts.get(predicate);
      if (counter == null) {
         counter = new Counter();
         suspendedPredicateSubscriptionCounts.put(predicate, counter);
      }
      counter.value++;
   }

   public int getSuspendedSubscriptionsCounter(Predicate<AttributeId> predicate) {
      if (isSingleFilter()) {
         return -1;
      }

      Counter counter = suspendedPredicateSubscriptionCounts.get(predicate);
      return counter == null ? 0 : counter.value;
   }

   public AttributeNode<AttributeMetadata, AttributeId> getRootNode() {
      return rootNode;
   }

   public void process(AttributeNode<AttributeMetadata, AttributeId> node) {
      currentNode = node;
      processAttributes(currentNode, instance);
   }

   public void notifySubscribers() {
      if (isSingleFilter()) {
         return;
      }

      for (int i = 0; i < filterContexts.length; i++) {
         FilterSubscriptionImpl s = filterSubscriptions.get(i);
         FilterEvalContext filterEvalContext = filterContexts[i];
         if (filterEvalContext == null) {
            if (s.getBETree().getChildCounters()[0] == BETree.EXPR_TRUE) {
               // this filter is a tautology and since its FilterEvalContext was never activated that means it also does not have projections
               filterEvalContext = new FilterEvalContext(this, s);
               filterContexts[i] = filterEvalContext;
            } else {
               continue;
            }
         }
         if (filterEvalContext.isMatching()) {
            s.getCallback().onFilterResult(false, userContext, eventType, instance, filterEvalContext.getProjection(), filterEvalContext.getSortProjection());
         }
      }
   }

   public void notifyDeltaSubscribers(MatcherEvalContext other, Object joiningEvent, Object leavingEvent) {
      if (isSingleFilter()) {
         return;
      }

      for (int i = 0; i < filterContexts.length; i++) {
         FilterSubscriptionImpl s = filterSubscriptions.get(i);

         FilterEvalContext filterEvalContext1 = filterContexts[i];
         if (filterEvalContext1 == null && s.getBETree().getChildCounters()[0] == BETree.EXPR_TRUE) {
            // this filter is a tautology and since its FilterEvalContext was never activated that means it also does not have projections
            filterEvalContext1 = new FilterEvalContext(this, s);
            filterContexts[i] = filterEvalContext1;
         }

         FilterEvalContext filterEvalContext2 = null;
         if (other != null) {
            filterEvalContext2 = other.filterContexts[i];
            if (filterEvalContext2 == null && s.getBETree().getChildCounters()[0] == BETree.EXPR_TRUE) {
               // this filter is a tautology and since its FilterEvalContext was never activated that means it also does not have projections
               filterEvalContext2 = new FilterEvalContext(other, s);
               other.filterContexts[i] = filterEvalContext2;
            }
         }

         boolean before = filterEvalContext1 != null && filterEvalContext1.isMatching();
         boolean after = filterEvalContext2 != null && filterEvalContext2.isMatching();
         if (!before && after) {
            s.getCallback().onFilterResult(true, userContext, joiningEvent, other.instance, filterEvalContext2.getProjection(), filterEvalContext2.getSortProjection());
         } else if (before && !after) {
            s.getCallback().onFilterResult(true, userContext, leavingEvent, instance, filterEvalContext1.getProjection(), filterEvalContext1.getSortProjection());
         }
      }
   }

   protected abstract void processAttributes(AttributeNode<AttributeMetadata, AttributeId> node, Object instance);

   private static final class Counter {
      int value;
   }
}
