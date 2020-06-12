package org.infinispan.query.remote.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.hibernate.search.spi.CustomTypeMetadata;
import org.hibernate.search.spi.IndexedTypeMap;
import org.hibernate.search.spi.impl.IndexedTypeMaps;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.impl.IndexedQuery;
import org.infinispan.query.impl.QueryDefinition;
import org.infinispan.query.remote.impl.filter.IckleProtobufFilterAndConverter;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.util.function.SerializableFunction;

/**
 * Same as ObjectRemoteQueryEngine but able to deal with protobuf payloads.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
final class RemoteQueryEngine extends ObjectRemoteQueryEngine {

   private static final SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> queryEngineProvider = c -> c.getComponentRegistry().getComponent(RemoteQueryManager.class).getQueryEngine(c);

   RemoteQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed) {
      super(isIndexed ? cache.withStorageMediaType().withWrapping(ByteArrayWrapper.class, ProtobufWrapper.class) : cache.withStorageMediaType(),
            isIndexed, ProtobufMatcher.class, new ProtobufFieldBridgeAndAnalyzerProvider());
   }

   @Override
   protected RowProcessor makeProjectionProcessor(Class<?>[] projectedTypes, Object[] projectedNullMarkers) {
      // Protobuf's booleans are indexed as Strings, so we need to convert them.
      // Collect here the positions of all Boolean projections.
      int[] booleanPositions = new int[projectedTypes.length];
      int booleanColumnsNumber = 0;
      for (int i = 0; i < projectedTypes.length; i++) {
         if (projectedTypes[i] == Boolean.class) {
            booleanPositions[booleanColumnsNumber++] = i;
         }
      }
      boolean hasNullMarkers = false;
      if (projectedNullMarkers != null) {
         for (Object projectedNullMarker : projectedNullMarkers) {
            if (projectedNullMarker != null) {
               hasNullMarkers = true;
               break;
            }
         }
      }
      if (booleanColumnsNumber == 0 && !hasNullMarkers) {
         return null;
      }
      final boolean hasNullMarkers_ = hasNullMarkers;
      final int[] booleanColumns = booleanColumnsNumber < booleanPositions.length ? Arrays.copyOf(booleanPositions, booleanColumnsNumber) : booleanPositions;
      return row -> {
         if (hasNullMarkers_) {
            for (int i = 0; i < projectedNullMarkers.length; i++) {
               if (row[i] != null && row[i].equals(projectedNullMarkers[i])) {
                  row[i] = null;
               }
            }
         }
         for (int i : booleanColumns) {
            if (row[i] != null) {
               // the Boolean column is actually encoded as a String, so we convert it to a boolean
               row[i] = "true".equals(row[i]);
            }
         }
         return row;
      };
   }

   @Override
   protected Query makeTypeQuery(Query query, String targetEntityName) {
      return new BooleanQuery.Builder()
            .add(new TermQuery(new Term(ProtobufValueWrapper.TYPE_FIELD_NAME, targetEntityName)), BooleanClause.Occur.FILTER)
            .add(query, BooleanClause.Occur.MUST)
            .build();
   }

   @Override
   protected IndexedQuery<?> makeCacheQuery(IckleParsingResult<Descriptor> ickleParsingResult, Query luceneQuery, IndexedQueryMode queryMode, Map<String, Object> namedParameters) {
      IndexingMetadata indexingMetadata = ickleParsingResult.getTargetEntityMetadata().getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
      Set<String> sortableFields = indexingMetadata != null ? indexingMetadata.getSortableFields() : Collections.emptySet();
      IndexedTypeMap<CustomTypeMetadata> queryMetadata = IndexedTypeMaps.singletonMapping(ProtobufValueWrapper.INDEXING_TYPE, () -> sortableFields);
      QueryDefinition queryDefinition;
      if (queryMode == IndexedQueryMode.BROADCAST) {
         queryDefinition = new QueryDefinition(ickleParsingResult.getQueryString(), queryEngineProvider);
      } else {
         queryDefinition = new QueryDefinition(getSearchFactory().createHSQuery(luceneQuery, queryMetadata));
      }
      queryDefinition.setNamedParameters(namedParameters);
      return (IndexedQuery<?>) getSearchManager().getQuery(queryDefinition, queryMode, queryMetadata);
   }

   @Override
   protected IckleFilterAndConverter createFilter(String queryString, Map<String, Object> namedParameters) {
      return isIndexed ? new IckleProtobufFilterAndConverter(queryString, namedParameters) :
            super.createFilter(queryString, namedParameters);
   }

   @Override
   protected Class<?> getTargetedClass(IckleParsingResult<?> parsingResult) {
      return ProtobufValueWrapper.class;
   }
}
