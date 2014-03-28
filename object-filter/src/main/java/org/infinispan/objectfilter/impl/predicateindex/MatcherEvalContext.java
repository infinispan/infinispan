package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class MatcherEvalContext<AttributeId extends Comparable<AttributeId>> {

   /**
    * Current node during traversal of the attribute tree.
    */
   protected AttributeNode<AttributeId> currentNode;

   /**
    * Each filter subscription has its own evaluation context, created on demand.
    */
   private final Map<FilterSubscriptionImpl, FilterEvalContext> filterContexts = new HashMap<FilterSubscriptionImpl, FilterEvalContext>();

   private final Map<Predicate<?>, AtomicInteger> suspendedSubscriptionCounts = new HashMap<Predicate<?>, AtomicInteger>();

   private final Object instance;

   protected String entityTypeName;

   protected MatcherEvalContext(Object instance) {
      this.instance = instance;
   }

   public String getEntityTypeName() {
      return entityTypeName;
   }

   public Object getInstance() {
      return instance;
   }

   public FilterEvalContext getFilterContext(FilterSubscriptionImpl filterSubscription) {
      FilterEvalContext filterEvalContext = filterContexts.get(filterSubscription);
      if (filterEvalContext == null) {
         filterEvalContext = new FilterEvalContext(filterSubscription.getBETree(), this);
         filterContexts.put(filterSubscription, filterEvalContext);
      }
      return filterEvalContext;
   }

   //todo [anistor] this is an AtomicInteger just because mutability is needed. there are no concurrency concerns here.
   public AtomicInteger getSuspendedSubscriptionsCounter(Predicate<?> predicate) {
      AtomicInteger counter = suspendedSubscriptionCounts.get(predicate);
      if (counter == null) {
         counter = new AtomicInteger();
         suspendedSubscriptionCounts.put(predicate, counter);
      }
      return counter;
   }

   public void process(AttributeNode<AttributeId> node) {
      currentNode = node;
      processAttributes(currentNode, instance);
   }

   protected abstract void processAttributes(AttributeNode<AttributeId> node, Object instance);
}
