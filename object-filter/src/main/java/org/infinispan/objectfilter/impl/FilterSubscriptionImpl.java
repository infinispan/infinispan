package org.infinispan.objectfilter.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.predicateindex.PredicateIndex;
import org.infinispan.objectfilter.impl.predicateindex.Predicates;
import org.infinispan.objectfilter.impl.predicateindex.be.BENode;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;
import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;
import org.infinispan.objectfilter.impl.util.ComparableArrayComparator;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> implements FilterSubscription {

   private final String queryString;

   private final Map<String, Object> namedParameters;

   private final boolean useIntervals;

   public int index = -1; // todo [anistor] hide this

   private final MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter;

   private final BETree beTree;

   private final List<Predicates.Subscription<AttributeId>> predicateSubscriptions = new ArrayList<>();

   private final FilterCallback callback;

   private final boolean isDeltaFilter;

   private final String[] projection;

   private final Class<?>[] projectionTypes;

   private final List<List<AttributeId>> translatedProjection;

   private final SortField[] sortFields;

   private final List<List<AttributeId>> translatedSortProjection;

   private final Object[] eventTypes;

   private Comparator<Comparable[]> comparator;

   protected FilterSubscriptionImpl(String queryString, Map<String, Object> namedParameters,
                                    boolean useIntervals, MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter,
                                    BETree beTree, FilterCallback callback, boolean isDeltaFilter,
                                    String[] projection, Class<?>[] projectionTypes, List<List<AttributeId>> translatedProjection,
                                    SortField[] sortFields, List<List<AttributeId>> translatedSortProjection,
                                    Object[] eventTypes) {
      this.queryString = queryString;
      this.namedParameters = namedParameters;
      this.useIntervals = useIntervals;
      this.metadataAdapter = metadataAdapter;
      this.beTree = beTree;
      this.callback = callback;
      this.isDeltaFilter = isDeltaFilter;
      this.projection = projection;
      this.projectionTypes = projectionTypes;
      this.sortFields = sortFields;
      this.translatedProjection = translatedProjection;
      this.translatedSortProjection = translatedSortProjection;
      this.eventTypes = eventTypes;
   }

   public String getQueryString() {
      return queryString;
   }

   public Map<String, Object> getNamedParameters() {
      return namedParameters;
   }

   public boolean useIntervals() {
      return useIntervals;
   }

   public BETree getBETree() {
      return beTree;
   }

   @Override
   public String getEntityTypeName() {
      return metadataAdapter.getTypeName();
   }

   public MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> getMetadataAdapter() {
      return metadataAdapter;
   }

   @Override
   public FilterCallback getCallback() {
      return callback;
   }

   @Override
   public boolean isDeltaFilter() {
      return isDeltaFilter;
   }

   @Override
   public String[] getProjection() {
      return projection;
   }

   public Class<?>[] getProjectionTypes() {
      return projectionTypes;
   }

   @Override
   public SortField[] getSortFields() {
      return sortFields;
   }

   @Override
   public Comparator<Comparable[]> getComparator() {
      if (sortFields != null && comparator == null) {
         boolean[] direction = new boolean[sortFields.length];
         for (int i = 0; i < sortFields.length; i++) {
            direction[i] = sortFields[i].isAscending();
         }
         comparator = new ComparableArrayComparator(direction);
      }
      return comparator;
   }

   @Override
   public Object[] getEventTypes() {
      return eventTypes;
   }

   public void registerProjection(PredicateIndex<AttributeMetadata, AttributeId> predicateIndex) {
      int i = 0;
      if (translatedProjection != null) {
         i = predicateIndex.addProjections(this, translatedProjection, i);
      }
      if (translatedSortProjection != null) {
         predicateIndex.addProjections(this, translatedSortProjection, i);
      }
   }

   public void unregisterProjection(PredicateIndex<AttributeMetadata, AttributeId> predicateIndex) {
      if (translatedProjection != null) {
         predicateIndex.removeProjections(this, translatedProjection);
      }
      if (translatedSortProjection != null) {
         predicateIndex.removeProjections(this, translatedSortProjection);
      }
   }

   public void subscribe(PredicateIndex<AttributeMetadata, AttributeId> predicateIndex) {
      for (BENode node : beTree.getNodes()) {
         if (node instanceof PredicateNode) {
            PredicateNode<AttributeId> predicateNode = (PredicateNode<AttributeId>) node;
            predicateSubscriptions.add(predicateIndex.addSubscriptionForPredicate(predicateNode, this));
         }
      }
   }

   public void unsubscribe(PredicateIndex<AttributeMetadata, AttributeId> predicateIndex) {
      for (Predicates.Subscription<AttributeId> subscription : predicateSubscriptions) {
         predicateIndex.removeSubscriptionForPredicate(subscription);
      }
   }
}
