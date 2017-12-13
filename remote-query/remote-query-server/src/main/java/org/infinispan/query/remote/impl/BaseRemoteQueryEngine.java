package org.infinispan.query.remote.impl;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryFactory;
import org.infinispan.query.dsl.embedded.impl.LuceneQueryMaker;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class BaseRemoteQueryEngine extends QueryEngine<Descriptor> {

   private final SerializationContext serializationContext;

   private final EmbeddedQueryFactory queryFactory = new EmbeddedQueryFactory(this);

   BaseRemoteQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed, Class<? extends Matcher> matcherImplClass,
                         LuceneQueryMaker.FieldBridgeAndAnalyzerProvider<Descriptor> fieldBridgeAndAnalyzerProvider) {
      super(cache, isIndexed, matcherImplClass, fieldBridgeAndAnalyzerProvider);
      serializationContext = ProtobufMetadataManagerImpl.getSerializationContextInternal(cache.getCacheManager());
   }

   protected SerializationContext getSerializationContext() {
      return serializationContext;
   }

   Query makeQuery(String queryString, Map<String, Object> namedParameters, long startOffset, int maxResults) {
      return makeQuery(queryString, namedParameters, startOffset, maxResults, IndexedQueryMode.FETCH);
   }

   Query makeQuery(String queryString, Map<String, Object> namedParameters, long startOffset, int maxResults, IndexedQueryMode queryMode) {
      Query query = queryFactory.create(queryString, queryMode);
      query.startOffset(startOffset);
      query.maxResults(maxResults);
      if (namedParameters != null) {
         query.setParameters(namedParameters);
      }
      return query;
   }
}
