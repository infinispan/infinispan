package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.PredicateIndex;
import org.infinispan.objectfilter.impl.predicateindex.be.BENode;
import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;

import java.util.Comparator;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class ObjectFilterImpl<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> implements ObjectFilter {

   private final BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId> matcher;

   private final FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscription;

   private final PredicateIndex<AttributeMetadata, AttributeId> predicateIndex;

   public ObjectFilterImpl(BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId> matcher, FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscription) {
      this.matcher = matcher;
      this.filterSubscription = filterSubscription;

      predicateIndex = new PredicateIndex<AttributeMetadata, AttributeId>(filterSubscription.getMetadataAdapter());

      filterSubscription.registerProjection(predicateIndex);

      for (BENode node : filterSubscription.getBETree().getNodes()) {
         if (node instanceof PredicateNode) {
            PredicateNode<AttributeId> predicateNode = (PredicateNode<AttributeId>) node;
            predicateIndex.addSubscriptionForPredicate(predicateNode, filterSubscription);
         }
      }
   }

   @Override
   public String getEntityTypeName() {
      return filterSubscription.getEntityTypeName();
   }

   @Override
   public String[] getProjection() {
      return filterSubscription.getProjection();
   }

   @Override
   public SortField[] getSortFields() {
      return filterSubscription.getSortFields();
   }

   @Override
   public Comparator<Comparable[]> getComparator() {
      return filterSubscription.getComparator();
   }

   @Override
   public FilterResult filter(Object instance) {
      if (instance == null) {
         throw new IllegalArgumentException("argument cannot be null");
      }

      MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> matcherEvalContext = matcher.startContext(instance, filterSubscription);
      if (matcherEvalContext != null) {
         FilterEvalContext filterEvalContext = matcherEvalContext.initSingleFilterContext(filterSubscription);
         matcherEvalContext.process(predicateIndex.getRoot());

         if (filterEvalContext.getMatchResult()) {
            return new FilterResultImpl(instance, filterEvalContext.getProjection(), filterEvalContext.getSortProjection());
         }
      }

      return null;
   }
}
