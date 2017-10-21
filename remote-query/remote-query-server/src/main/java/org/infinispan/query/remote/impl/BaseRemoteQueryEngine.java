package org.infinispan.query.remote.impl;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryFactory;
import org.infinispan.query.dsl.embedded.impl.LuceneQueryMaker;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.dsl.impl.BaseQuery;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class BaseRemoteQueryEngine extends QueryEngine<Descriptor> {

   private final SerializationContext serializationContext;

   private final EmbeddedQueryFactory queryFactory = new EmbeddedQueryFactory(this);

   protected BaseRemoteQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed, Class<? extends Matcher> matcherImplClass,
                                   LuceneQueryMaker.FieldBridgeAndAnalyzerProvider<Descriptor> fieldBridgeAndAnalyzerProvider) {
      super(cache, isIndexed, matcherImplClass, fieldBridgeAndAnalyzerProvider);
      serializationContext = ProtobufMetadataManagerImpl.getSerializationContextInternal(cache.getCacheManager());
   }

   protected SerializationContext getSerializationContext() {
      return serializationContext;
   }

   public BaseQuery makeQuery(String queryString, Map<String, Object> namedParameters, long startOffset, int maxResults) {
      BaseQuery query = queryFactory.create(queryString);
      query.startOffset(startOffset);
      query.maxResults(maxResults);
      if (namedParameters != null) {
         query.setParameters(namedParameters);
      }
      return query;
   }
}
