package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.predicateindex.AttributeNode;
import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.PredicateIndex;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;
import org.infinispan.objectfilter.impl.predicateindex.be.BETreeMaker;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanFilterNormalizer;
import org.infinispan.objectfilter.impl.util.StringHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class ObjectFilterImpl<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> implements ObjectFilter {

   private static final FilterCallback emptyCallback = (isDelta, userContext, eventType, instance, projection, sortProjection) -> {
      // do nothing
   };

   private final BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId> matcher;

   private final MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter;

   private final FieldAccumulator[] acc;

   private final BooleanExpr query;

   private final Set<String> paramNames;

   private final Map<String, Object> namedParameters;

   private final String queryString;

   private final String[] projection;

   private final Class<?>[] projectionTypes;

   private final List<List<AttributeId>> translatedProjections;

   private final SortField[] sortFields;

   private final List<List<AttributeId>> translatedSortFields;

   private final BooleanExpr normalizedQuery;

   private FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscription;

   private AttributeNode<AttributeMetadata, AttributeId> root;

   ObjectFilterImpl(BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId> matcher,
                    MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter,
                    String queryString, Set<String> paramNames, BooleanExpr query,
                    String[] projection, Class<?>[] projectionTypes, SortField[] sortFields, FieldAccumulator[] acc) {
      if (acc != null) {
         if (projectionTypes == null) {
            throw new IllegalArgumentException("Accumulators can only be used with projections");
         }
         if (sortFields != null) {
            throw new IllegalArgumentException("Accumulators cannot be used with sorting");
         }
      }

      this.namedParameters = null;
      this.paramNames = paramNames;
      this.matcher = matcher;
      this.metadataAdapter = metadataAdapter;
      this.acc = acc;
      this.query = query;
      this.queryString = queryString;
      this.projection = projection;
      this.projectionTypes = projectionTypes;
      this.sortFields = sortFields;

      if (projection != null && projection.length != 0) {
         translatedProjections = new ArrayList<>(projection.length);
         for (String projectionPath : projection) {
            translatedProjections.add(metadataAdapter.translatePropertyPath(StringHelper.split(projectionPath)));
         }
      } else {
         translatedProjections = null;
      }

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
            translatedSortFields.add(metadataAdapter.translatePropertyPath(sortField.getPath().getPath()));
         }
      } else {
         translatedSortFields = null;
      }

      BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();
      normalizedQuery = booleanFilterNormalizer.normalize(query);

      if (paramNames.isEmpty()) {
         subscribe();
      }
   }

   private ObjectFilterImpl(ObjectFilterImpl<TypeMetadata, AttributeMetadata, AttributeId> other, Map<String, Object> namedParameters) {
      this.namedParameters = Collections.unmodifiableMap(namedParameters);

      this.paramNames = other.paramNames;
      this.matcher = other.matcher;
      this.metadataAdapter = other.metadataAdapter;
      this.acc = other.acc;
      this.query = other.query;
      this.queryString = other.queryString;
      this.projection = other.projection;
      this.projectionTypes = other.projectionTypes;
      this.sortFields = other.sortFields;
      this.translatedProjections = other.translatedProjections;
      this.translatedSortFields = other.translatedSortFields;
      this.normalizedQuery = other.normalizedQuery;

      subscribe();
   }

   private void subscribe() {
      BETreeMaker<AttributeId> treeMaker = new BETreeMaker<>(metadataAdapter, false);
      BETree beTree = treeMaker.make(normalizedQuery, namedParameters);

      PredicateIndex<AttributeMetadata, AttributeId> predicateIndex = new PredicateIndex<>(metadataAdapter);
      root = predicateIndex.getRoot();

      filterSubscription = new FilterSubscriptionImpl<>(queryString, namedParameters, false, metadataAdapter, beTree,
            emptyCallback, projection, projectionTypes, translatedProjections, sortFields, translatedSortFields, null);
      filterSubscription.registerProjection(predicateIndex);
      filterSubscription.subscribe(predicateIndex);
      filterSubscription.index = 0;
   }

   @Override
   public String getEntityTypeName() {
      return metadataAdapter.getTypeName();
   }

   @Override
   public String[] getProjection() {
      return projection;
   }

   @Override
   public Class<?>[] getProjectionTypes() {
      return projectionTypes;
   }

   @Override
   public Set<String> getParameterNames() {
      return paramNames;
   }

   @Override
   public Map<String, Object> getParameters() {
      return namedParameters;
   }

   @Override
   public ObjectFilter withParameters(Map<String, Object> namedParameters) {
      if (namedParameters == null) {
         throw new IllegalArgumentException("namedParameters argument cannot be null");
      }
      //todo validate params
      return new ObjectFilterImpl<>(this, namedParameters);
   }

   @Override
   public SortField[] getSortFields() {
      return sortFields;
   }

   @Override
   public Comparator<Comparable[]> getComparator() {
      if (filterSubscription == null) {
         throw new IllegalStateException("Parameter values were not bound yet.");
      }

      return filterSubscription.getComparator();
   }

   @Override
   public FilterResult filter(Object instance) {
      if (filterSubscription == null) {
         throw new IllegalStateException("Parameter values were not bound yet.");
      }

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
