package org.infinispan.query.remote;

import org.apache.lucene.search.Query;
import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.lucene.LuceneProcessingChain;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.hibernate.hql.lucene.spi.FieldBridgeProvider;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.StringBridge;
import org.hibernate.search.bridge.builtin.impl.NullEncodingTwoWayFieldBridge;
import org.hibernate.search.bridge.builtin.impl.TwoWayString2FieldBridgeAdaptor;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQuery;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;
import org.infinispan.query.remote.indexing.IndexingMetadata;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.infinispan.server.core.QueryFacade;
import org.infinispan.util.KeyValuePair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A query facade implementation for both Lucene based queries and non-indexed queries.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class QueryFacadeImpl implements QueryFacade {

   /**
    * A special hidden Lucene document field that holds the actual protobuf type name.
    */
   public static final String TYPE_FIELD_NAME = "$type$";

   /**
    * A special placeholder value that is indexed if the actual field value is null. This placeholder is needed because
    * Lucene does not index null values.
    */
   public static final String NULL_TOKEN = "_null_";

   @Override
   public byte[] query(AdvancedCache<byte[], byte[]> cache, byte[] query) {
      try {
         SerializationContext serCtx = ProtobufMetadataManager.getSerializationContextInternal(cache.getCacheManager());
         QueryRequest request = ProtobufUtil.fromByteArray(serCtx, query, 0, query.length, QueryRequest.class);

         QueryResponse response;
         if (cache.getCacheConfiguration().indexing().index().isEnabled()) {
            response = executeQuery(cache, serCtx, request);
         } else {
            response = executeNonIndexedQuery(cache, serCtx, request);
         }

         return ProtobufUtil.toByteArray(serCtx, response);
      } catch (IOException e) {
         throw new CacheException("An exception has occurred during query execution", e);
      }
   }

   private QueryResponse executeNonIndexedQuery(AdvancedCache<byte[], byte[]> cache, SerializationContext serCtx, QueryRequest request) throws IOException {
      boolean compatMode = cache.getCacheConfiguration().compatibility().enabled();
      Class<? extends Matcher> matcherImplClass = compatMode ? CompatibilityReflectionMatcher.class : ProtobufMatcher.class;

      EmbeddedQuery eq = new EmbeddedQuery(cache, request.getJpqlString(), request.getStartOffset(), request.getMaxResults(), matcherImplClass);
      List<?> list = eq.list();
      int projSize = 0;
      if (eq.getProjection() != null && eq.getProjection().length > 0) {
         projSize = eq.getProjection().length;
      }
      List<WrappedMessage> results = new ArrayList<WrappedMessage>(projSize == 0 ? list.size() : list.size() * projSize);
      for (Object o : list) {
         if (projSize == 0) {
            if (compatMode) {
               // if we are in compat mode then this is the real object so need to marshall it first
               o = ProtobufUtil.toWrappedByteArray(serCtx, o);
            }
            results.add(new WrappedMessage(o));
         } else {
            Object[] row = (Object[]) o;
            for (int j = 0; j < projSize; j++) {
               results.add(new WrappedMessage(row[j]));
            }
         }
      }

      QueryResponse response = new QueryResponse();
      response.setTotalResults(eq.getResultSize());
      response.setNumResults(list.size());
      response.setProjectionSize(projSize);
      response.setResults(results);

      return response;
   }

   /**
    * Execute Lucene index query.
    */
   private QueryResponse executeQuery(AdvancedCache<byte[], byte[]> cache, SerializationContext serCtx, QueryRequest request) {
      final SearchManager searchManager = Search.getSearchManager(cache);
      final SearchFactoryIntegrator searchFactory = (SearchFactoryIntegrator) searchManager.getSearchFactory();
      final QueryCache queryCache = ComponentRegistryUtils.getQueryCache(cache);  // optional component

      LuceneQueryParsingResult parsingResult;
      Query luceneQuery;

      if (queryCache != null) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(request.getJpqlString(), LuceneQueryParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = parseQuery(cache, serCtx, request.getJpqlString(), searchFactory);
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = parseQuery(cache, serCtx, request.getJpqlString(), searchFactory);
      }

      luceneQuery = parsingResult.getQuery();

      if (!cache.getCacheConfiguration().compatibility().enabled()) {
         // restrict on entity type
         QueryBuilder qb = searchFactory.buildQueryBuilder().forEntity(parsingResult.getTargetEntity()).get();
         luceneQuery = qb.bool()
               .must(qb.keyword().onField(TYPE_FIELD_NAME)
                           .ignoreFieldBridge()
                           .ignoreAnalyzer()
                           .matching(parsingResult.getTargetEntityName()).createQuery())
               .must(luceneQuery)
               .createQuery();
      }

      CacheQuery cacheQuery = searchManager.getQuery(luceneQuery, parsingResult.getTargetEntity());

      if (parsingResult.getSort() != null) {
         cacheQuery = cacheQuery.sort(parsingResult.getSort());
      }

      int projSize = 0;
      if (parsingResult.getProjections() != null && !parsingResult.getProjections().isEmpty()) {
         projSize = parsingResult.getProjections().size();
         cacheQuery = cacheQuery.projection(parsingResult.getProjections().toArray(new String[projSize]));
      }
      if (request.getStartOffset() > 0) {
         cacheQuery = cacheQuery.firstResult((int) request.getStartOffset());
      }
      if (request.getMaxResults() > 0) {
         cacheQuery = cacheQuery.maxResults(request.getMaxResults());
      }

      List<?> list = cacheQuery.list();
      List<WrappedMessage> results = new ArrayList<WrappedMessage>(projSize == 0 ? list.size() : list.size() * projSize);
      for (Object o : list) {
         if (projSize == 0) {
            results.add(new WrappedMessage(o));
         } else {
            Object[] row = (Object[]) o;
            for (int j = 0; j < projSize; j++) {
               results.add(new WrappedMessage(row[j]));
            }
         }
      }

      QueryResponse response = new QueryResponse();
      response.setTotalResults(cacheQuery.getResultSize());
      response.setNumResults(list.size());
      response.setProjectionSize(projSize);
      response.setResults(results);

      return response;
   }

   private LuceneQueryParsingResult parseQuery(AdvancedCache<byte[], byte[]> cache, final SerializationContext serCtx, String queryString, SearchFactoryIntegrator searchFactory) {
      LuceneProcessingChain processingChain;
      if (cache.getCacheConfiguration().compatibility().enabled()) {
         final QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
         EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
            @Override
            public Class<?> getClassFromName(String entityName) {
               MessageMarshaller messageMarshaller = (MessageMarshaller) serCtx.getMarshaller(entityName);
               Class clazz = messageMarshaller.getJavaClass();
               return queryInterceptor.isIndexed(clazz) ? clazz : null;
            }
         };

         processingChain = new LuceneProcessingChain.Builder(searchFactory, entityNamesResolver)
               .buildProcessingChainForClassBasedEntities();
      } else {
         EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
            @Override
            public Class<?> getClassFromName(String entityName) {
               return serCtx.canMarshall(entityName) ? ProtobufValueWrapper.class : null;
            }
         };

         FieldBridgeProvider fieldBridgeProvider = new FieldBridgeProvider() {
            @Override
            public FieldBridge getFieldBridge(String type, String propertyPath) {
               Descriptor md = serCtx.getMessageDescriptor(type);
               FieldDescriptor fd = getFieldDescriptor(md, propertyPath);
               IndexingMetadata indexingMetadata = md.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
               if (indexingMetadata != null && !indexingMetadata.isFieldIndexed(fd.getNumber())) {
                  throw new IllegalArgumentException("Field " + propertyPath + " from type " + md.getFullName() + " is not indexed");
               }

               switch (fd.getType()) {
                  case DOUBLE:
                     return NumericFieldBridge.DOUBLE_FIELD_BRIDGE;
                  case FLOAT:
                     return NumericFieldBridge.FLOAT_FIELD_BRIDGE;
                  case INT64:
                  case UINT64:
                  case FIXED64:
                  case SFIXED64:
                  case SINT64:
                     return NumericFieldBridge.LONG_FIELD_BRIDGE;
                  case INT32:
                  case FIXED32:
                  case UINT32:
                  case SFIXED32:
                  case SINT32:
                  case BOOL:
                  case ENUM:
                     return NumericFieldBridge.INT_FIELD_BRIDGE;
                  case STRING:
                  case BYTES:
                  case GROUP:
                  case MESSAGE:
                     return new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(StringBridge.INSTANCE), NULL_TOKEN);
               }
               return null;
            }
         };

         processingChain = new LuceneProcessingChain.Builder(searchFactory, entityNamesResolver)
               .buildProcessingChainForDynamicEntities(fieldBridgeProvider);
      }

      return new QueryParser().parseQuery(queryString, processingChain);
   }

   private FieldDescriptor getFieldDescriptor(Descriptor messageDescriptor, String attributePath) {
      FieldDescriptor fd = null;
      String[] split = attributePath.split("[.]");
      for (int i = 0; i < split.length; i++) {
         String name = split[i];
         fd = messageDescriptor.findFieldByName(name);
         if (fd == null) {
            throw new IllegalArgumentException("Unknown field " + name + " in type " + messageDescriptor.getFullName());
         }
         if (i < split.length - 1) {
            messageDescriptor = fd.getMessageType();
         }
      }
      return fd;
   }
}
