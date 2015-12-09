package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.predicateindex.AttributeNode;
import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

import java.util.Comparator;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class ObjectFilterImpl<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> implements ObjectFilter {

   private final BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId> matcher;

   private final FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscription;

   private final AttributeNode<AttributeMetadata, AttributeId> root;

   private final FieldAccumulator[] acc;

   private static final FilterCallback emptyCallback = new FilterCallback() {
      @Override
      public void onFilterResult(boolean isDelta, Object userContext, Object eventType, Object instance, Object[] projection, Comparable[] sortProjection) {
         // do nothing
      }
   };

   ObjectFilterImpl(BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId> matcher,
                    MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter,
                    String queryString, Map<String, Object> namedParameters, BooleanExpr query,
                    String[] projections, Class<?>[] projectionTypes, SortField[] sortFields, FieldAccumulator[] acc) {
      if (acc != null) {
         if (projectionTypes == null) {
            throw new IllegalArgumentException("Accumulators can only be used with projections");
         }
         if (sortFields != null) {
            throw new IllegalArgumentException("Accumulators cannot be used with sorting");
         }
      }

      this.matcher = matcher;
      this.acc = acc;

      //todo [anistor] we need an efficient single-filter registry
      FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> filterRegistry = new FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>(metadataAdapter, false);
      filterSubscription = filterRegistry.addFilter(queryString, namedParameters, query, projections, projectionTypes, sortFields, emptyCallback, null);
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
   public Class<?>[] getProjectionTypes() {
      return filterSubscription.getProjectionTypes();
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
         throw new IllegalArgumentException("instance cannot be null");
      }

      MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> matcherEvalContext = matcher.startSingleTypeContext(null, null, instance, filterSubscription.getMetadataAdapter());
      if (matcherEvalContext != null) {
         FilterEvalContext filterEvalContext = matcherEvalContext.initSingleFilterContext(filterSubscription);
         if (acc != null) {
            filterEvalContext.acc = acc;
            for (FieldAccumulator a : acc) {
               if (a != null) {
                  a.init(filterEvalContext.getProjection());
               }
            }
         }
         matcherEvalContext.process(root);

         if (filterEvalContext.isMatching()) {
            Object o = filterEvalContext.getProjection() == null ? matcher.convert(instance) : null;
            return new FilterResultImpl(o, filterEvalContext.getProjection(), filterEvalContext.getSortProjection());
         }
      }

      return null;
   }
}
