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
import org.infinispan.objectfilter.impl.syntax.BooleanFilterNormalizer;
import org.infinispan.objectfilter.impl.util.StringHelper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A registry for filters on the same type of entity.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> {

   private final PredicateIndex<AttributeMetadata, AttributeId> predicateIndex;

   private final List<FilterSubscriptionImpl> filterSubscriptions = new ArrayList<FilterSubscriptionImpl>();

   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

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
         if (filterEvalContext.isMatching()) {
            // check if event type is matching
            s.getCallback().onFilterResult(ctx.getUserContext(), ctx.getInstance(), ctx.getEventType(), filterEvalContext.getProjection(), filterEvalContext.getSortProjection());
         }
      }
   }

   public FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> addFilter(String queryString, Map<String, Object> namedParameters, BooleanExpr query, String[] projection, Class<?>[] projectionTypes, SortField[] sortFields, FilterCallback callback, Object[] eventTypes) {
      if (eventTypes != null) {
         if (eventTypes.length == 0) {
            eventTypes = null;
         } else {
            for (Object et : eventTypes) {
               if (et == null) {
                  eventTypes = null;
                  break;
               }
            }
         }
      }

      List<List<AttributeId>> translatedProjections = null;
      if (projection != null && projection.length != 0) {
         translatedProjections = new ArrayList<List<AttributeId>>(projection.length);
         for (String projectionPath : projection) {
            translatedProjections.add(metadataAdapter.translatePropertyPath(StringHelper.splitPropertyPath(projectionPath)));
         }
      }

      List<List<AttributeId>> translatedSortFields = null;
      if (sortFields != null) {
         // deduplicate sort fields
         LinkedHashMap<String, SortField> sortFieldMap = new LinkedHashMap<String, SortField>();
         for (SortField sf : sortFields) {
            String path = sf.getPath().asStringPath();
            if (!sortFieldMap.containsKey(path)) {
               sortFieldMap.put(path, sf);
            }
         }
         sortFields = sortFieldMap.values().toArray(new SortField[sortFieldMap.size()]);
         // translate sort field paths
         translatedSortFields = new ArrayList<List<AttributeId>>(sortFields.length);
         for (SortField sortField : sortFields) {
            translatedSortFields.add(metadataAdapter.translatePropertyPath(sortField.getPath().getPath()));
         }
      }

      query = booleanFilterNormalizer.normalize(query);
      BETree beTree = treeMaker.make(query);

      FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscription = new FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId>(queryString, namedParameters, useIntervals, metadataAdapter, beTree, callback, projection, projectionTypes, translatedProjections, sortFields, translatedSortFields, eventTypes);
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
