package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.PredicateIndex;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;
import org.infinispan.objectfilter.impl.predicateindex.be.BETreeMaker;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.util.StringHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * A registry for filters on the same type of entity.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> {

   private final PredicateIndex<AttributeMetadata, AttributeId> predicateIndex;

   private final List<FilterSubscriptionImpl> filterSubscriptions = new ArrayList<FilterSubscriptionImpl>();

   private final BETreeMaker<AttributeId> treeMaker;

   private final MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter;

   private final boolean useIntervals;

   public FilterRegistry(MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter, boolean useIntervals) {
      this.metadataAdapter = metadataAdapter;
      this.useIntervals = useIntervals;
      treeMaker = new BETreeMaker<AttributeId>(metadataAdapter, useIntervals);
      predicateIndex = new PredicateIndex<AttributeMetadata, AttributeId>(metadataAdapter);
   }

   public MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> getMetadataAdapter() {
      return metadataAdapter;
   }

   public PredicateIndex<AttributeMetadata, AttributeId> getPredicateIndex() {
      return predicateIndex;
   }

   public int getNumFilters() {
      return filterSubscriptions.size();
   }

   /**
    * Executes the registered filters and notifies each one of them whether it was satisfied or not by the given
    * instance.
    *
    * @param ctx
    */
   public void match(MatcherEvalContext<?, AttributeMetadata, AttributeId> ctx) {
      // try to match
      ctx.process(predicateIndex.getRoot());

      // notify subscribers
      for (FilterSubscriptionImpl s : filterSubscriptions) {
         FilterEvalContext filterEvalContext = ctx.getFilterEvalContext(s);
         if (filterEvalContext.getMatchResult()) {
            s.getCallback().onFilterResult(ctx.getInstance(), filterEvalContext.getProjection(), filterEvalContext.getSortProjection());
         }
      }
   }

   public FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> addFilter(String queryString, BooleanExpr normalizedFilter, List<String> projection, List<SortField> sortFields, FilterCallback callback) {
      List<List<AttributeId>> translatedProjections = null;
      if (projection != null && !projection.isEmpty()) {
         translatedProjections = new ArrayList<List<AttributeId>>(projection.size());
         for (String projectionPath : projection) {
            translatedProjections.add(metadataAdapter.translatePropertyPath(StringHelper.splitPropertyPath(projectionPath)));
         }
      }

      List<List<AttributeId>> translatedSortFields = null;
      if (sortFields != null && !sortFields.isEmpty()) {
         translatedSortFields = new ArrayList<List<AttributeId>>(sortFields.size());
         for (SortField sortField : sortFields) {
            translatedSortFields.add(metadataAdapter.translatePropertyPath(StringHelper.splitPropertyPath(sortField.getPath())));
         }
      }

      BETree beTree = treeMaker.make(normalizedFilter);
      FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscription = new FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId>(queryString, useIntervals, metadataAdapter, beTree, callback, projection, translatedProjections, sortFields, translatedSortFields);
      filterSubscription.registerProjection(predicateIndex);
      filterSubscription.subscribe(predicateIndex);
      filterSubscription.index = filterSubscriptions.size();
      filterSubscriptions.add(filterSubscription);
      return filterSubscription;
   }

   public void removeFilter(FilterSubscription filterSubscription) {
      FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscriptionImpl = (FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId>) filterSubscription;
      filterSubscriptionImpl.unregisterProjection(predicateIndex);
      filterSubscriptionImpl.unsubscribe(predicateIndex);
      filterSubscriptions.remove(filterSubscriptionImpl);
      for (int i = filterSubscriptionImpl.index; i < filterSubscriptions.size(); i++) {
         filterSubscriptions.get(i).index--;
      }
      filterSubscriptionImpl.index = -1;
   }
}
