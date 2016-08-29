package org.infinispan.objectfilter.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.predicateindex.PredicateIndex;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;
import org.infinispan.objectfilter.impl.predicateindex.be.BETreeMaker;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanFilterNormalizer;
import org.infinispan.objectfilter.impl.util.StringHelper;

/**
 * A registry for filters on the same type of entity.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> {

   private final PredicateIndex<AttributeMetadata, AttributeId> predicateIndex;

   private final List<FilterSubscriptionImpl> filterSubscriptions = new ArrayList<>();

   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   private final BETreeMaker<AttributeId> treeMaker;

   private final MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter;

   private final boolean useIntervals;

   public FilterRegistry(MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter, boolean useIntervals) {
      this.metadataAdapter = metadataAdapter;
      this.useIntervals = useIntervals;
      treeMaker = new BETreeMaker<>(metadataAdapter, useIntervals);
      predicateIndex = new PredicateIndex<>(metadataAdapter);
   }

   public MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> getMetadataAdapter() {
      return metadataAdapter;
   }

   public PredicateIndex<AttributeMetadata, AttributeId> getPredicateIndex() {
      return predicateIndex;
   }

   public List<FilterSubscriptionImpl> getFilterSubscriptions() {
      return filterSubscriptions;
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
         translatedProjections = new ArrayList<>(projection.length);
         for (String projectionPath : projection) {
            translatedProjections.add(metadataAdapter.mapPropertyNamePathToFieldIdPath(StringHelper.split(projectionPath)));
         }
      }

      List<List<AttributeId>> translatedSortFields = null;
      if (sortFields != null) {
         // deduplicate sort fields
         LinkedHashMap<String, SortField> sortFieldMap = new LinkedHashMap<>();
         for (SortField sf : sortFields) {
            String path = sf.getPath().asStringPath();
            if (!sortFieldMap.containsKey(path)) {
               sortFieldMap.put(path, sf);
            }
         }
         sortFields = sortFieldMap.values().toArray(new SortField[sortFieldMap.size()]);
         // translate sort field paths
         translatedSortFields = new ArrayList<>(sortFields.length);
         for (SortField sortField : sortFields) {
            translatedSortFields.add(metadataAdapter.mapPropertyNamePathToFieldIdPath(sortField.getPath().asArrayPath()));
         }
      }

      BooleanExpr normalizedQuery = booleanFilterNormalizer.normalize(query);
      BETree beTree = treeMaker.make(normalizedQuery, namedParameters);

      FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscription = new FilterSubscriptionImpl<>(queryString, namedParameters, useIntervals, metadataAdapter, beTree, callback, projection, projectionTypes, translatedProjections, sortFields, translatedSortFields, eventTypes);
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
