package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;
import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

   private final Set<PredicateNode<AttributeId>> suspendedSubscriptions = new HashSet<PredicateNode<AttributeId>>();

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

   public FilterEvalContext getFilterEvalContext(FilterSubscriptionImpl filterSubscription) {
      FilterEvalContext filterEvalContext = filterContexts.get(filterSubscription);
      if (filterEvalContext == null) {
         Object[] projection = filterSubscription.getProjection() != null ? new Object[filterSubscription.getProjection().length] : null;
         filterEvalContext = new FilterEvalContext(filterSubscription.getBETree(), this, projection);
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

   public int getSuspendedSubscriptionsCounter(Predicate<?> predicate) {
      AtomicInteger counter = suspendedSubscriptionCounts.get(predicate);
      return counter == null ? 0 : counter.get();
   }

   public void process(AttributeNode<AttributeId> node) {
      currentNode = node;
      processAttributes(currentNode, instance);
   }

   protected abstract void processAttributes(AttributeNode<AttributeId> node, Object instance);
}
