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
import org.hibernate.search.query.engine.spi.HSQuery;
import org.hibernate.search.spi.CustomTypeMetadata;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.IndexedTypeMap;
import org.hibernate.search.spi.impl.IndexedTypeMaps;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.dsl.embedded.impl.IckleFilterAndConverter;
import org.infinispan.query.dsl.embedded.impl.RowProcessor;
import org.infinispan.query.impl.SearchManagerImpl;
import org.infinispan.query.remote.impl.filter.IckleProtobufFilterAndConverter;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
final class RemoteQueryEngine extends BaseRemoteQueryEngine {

   RemoteQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed) {
      super(isIndexed ? cache.withWrapping(ByteArrayWrapper.class, ProtostreamWrapper.class) : cache,
            isIndexed, ProtobufMatcher.class, new ProtobufFieldBridgeAndAnalyzerProvider());
   }

   @Override
   protected RowProcessor makeProjectionProcessor(Class<?>[] projectedTypes) {
      // Protobuf's booleans are indexed as Strings, so we need to convert them.
      // Collect here the positions of all Boolean projections.
      int[] pos = new int[projectedTypes.length];
      int len = 0;
      for (int i = 0; i < projectedTypes.length; i++) {
         if (projectedTypes[i] == Boolean.class) {
            pos[len++] = i;
         }
      }
      if (len == 0) {
         return null;
      }
      final int[] cols = len < pos.length ? Arrays.copyOf(pos, len) : pos;
      return row -> {
         for (int i : cols) {
            if (row[i] != null) {
               // the Boolean column is actually encoded as a String, so we convert it
               row[i] = "true".equals(row[i]);
            }
         }
         return row;
      };
   }

   @Override
   protected Query makeTypeQuery(Query query, String targetEntityName) {
      return new BooleanQuery.Builder()
            .add(new TermQuery(new Term(QueryFacadeImpl.TYPE_FIELD_NAME, targetEntityName)), BooleanClause.Occur.FILTER)
            .add(query, BooleanClause.Occur.MUST)
            .build();
   }

   @Override
   protected CacheQuery<?> makeCacheQuery(IckleParsingResult<Descriptor> ickleParsingResult, Query luceneQuery) {
      CustomTypeMetadata customTypeMetadata = new CustomTypeMetadata() {
         @Override
         public Set<String> getSortableFields() {
            IndexingMetadata indexingMetadata = ickleParsingResult.getTargetEntityMetadata().getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
            return indexingMetadata != null ? indexingMetadata.getSortableFields() : Collections.emptySet();
         }
      };
      IndexedTypeMap<CustomTypeMetadata> queryMetadata = IndexedTypeMaps.singletonMapping(ProtobufValueWrapper.INDEXING_TYPE, customTypeMetadata);
      HSQuery hSearchQuery = getSearchFactory().createHSQuery(luceneQuery, queryMetadata);
      return ((SearchManagerImpl) getSearchManager()).getQuery(hSearchQuery);
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
