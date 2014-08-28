package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.predicateindex.AttributeNode;
import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

import java.util.Comparator;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class ObjectFilterImpl<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> implements ObjectFilter {

   private final BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId> matcher;

   private final FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscription;

   private final AttributeNode<AttributeMetadata, AttributeId> root;

   private static final FilterCallback emptyCallback = new FilterCallback() {
      @Override
      public void onFilterResult(Object instance, Object[] projection, Comparable[] sortProjection) {
         // do nothing
      }
   };

   public ObjectFilterImpl(BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId> matcher,
                           MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter,
                           String jpaQuery, FilterParsingResult<TypeMetadata> parsingResult, BooleanExpr normalizedFilter) {
      this.matcher = matcher;

      //todo [anistor] we need an efficient single-filter registry
      FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> filterRegistry = new FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>(metadataAdapter, false);
      filterSubscription = filterRegistry.addFilter(jpaQuery, normalizedFilter, parsingResult.getProjections(), parsingResult.getSortFields(), emptyCallback);
      root = filterRegistry.getPredicateIndex().getRoot();
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
         matcherEvalContext.process(root);

         if (filterEvalContext.getMatchResult()) {
            return new FilterResultImpl(instance, filterEvalContext.getProjection(), filterEvalContext.getSortProjection());
         }
      }

      return null;
   }
}
