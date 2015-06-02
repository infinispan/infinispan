package org.infinispan.query.remote;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.hibernate.hql.ParsingException;
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
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQuery;
import org.infinispan.query.dsl.embedded.impl.JPAFilterAndConverter;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;
import org.infinispan.query.remote.filter.JPAProtobufFilterAndConverter;
import org.infinispan.query.remote.indexing.IndexingMetadata;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.infinispan.query.remote.logging.Log;
import org.infinispan.server.core.QueryFacade;
import org.infinispan.util.KeyValuePair;
import org.kohsuke.MetaInfServices;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

/**
 * A query facade implementation for both Lucene based queries and non-indexed queries.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@MetaInfServices
public final class QueryFacadeImpl implements QueryFacade {

   private static final Log log = LogFactory.getLog(QueryFacadeImpl.class, Log.class);

   /**
    * A special hidden Lucene document field that holds the actual protobuf type name.
    */
   public static final String TYPE_FIELD_NAME = "$type$";

   /**
    * A special placeholder value that is indexed if the actual field value is null. This placeholder is needed because
    * Lucene does not index null values.
    */
   public static final String NULL_TOKEN = "_null_";

   private final QueryParser queryParser = new QueryParser();

   @Override
   public byte[] query(AdvancedCache<byte[], byte[]> cache, byte[] query) {
      try {
         SerializationContext serCtx = ProtobufMetadataManager.getSerializationContextInternal(cache.getCacheManager());
         QueryRequest request = ProtobufUtil.fromByteArray(serCtx, query, 0, query.length, QueryRequest.class);

         Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache);
         QueryResponse response;
         if (cacheConfiguration.indexing().index().isEnabled()) {
            try {
               response = executeIndexedQuery(cache, cacheConfiguration, serCtx, request);
            } catch (IllegalArgumentException e) {
               if (e.getMessage().contains("ISPN018002:") || e.getMessage().contains("HQL100001:")) {
                  response = executeNonIndexedQuery(cache, cacheConfiguration, serCtx, request);
               } else {
                  throw e;
               }
            } catch (ParsingException e) {
               if (e.getMessage().contains("HQL100002:")) {
                  response = executeNonIndexedQuery(cache, cacheConfiguration, serCtx, request);
               } else {
                  throw e;
               }
            }
         } else {
            response = executeNonIndexedQuery(cache, cacheConfiguration, serCtx, request);
         }

         return ProtobufUtil.toByteArray(serCtx, response);
      } catch (IOException e) {
         throw log.errorExecutingQuery(e);
      }
   }

   private QueryResponse executeNonIndexedQuery(AdvancedCache<byte[], byte[]> cache, Configuration cacheConfiguration, SerializationContext serCtx, QueryRequest request) throws IOException {
      final boolean isIndexed = cacheConfiguration.indexing().index().isEnabled();
      final boolean isCompatMode = cacheConfiguration.compatibility().enabled();

      EmbeddedQuery eq = new EmbeddedQuery(null, cache, makeFilter(cache, isIndexed, isCompatMode, request.getJpqlString()), request.getStartOffset(), request.getMaxResults());
      List<?> list = eq.list();

      int projSize = eq.getProjection() != null && eq.getProjection().length > 0 ? eq.getProjection().length : 0;

      return makeResponse(isCompatMode, serCtx, list, projSize, eq.getResultSize(), list.size());
   }

   private JPAFilterAndConverter makeFilter(final AdvancedCache<?, ?> cache, final boolean isIndexed, final boolean isCompatMode, final String jpaQuery) {
      return SecurityActions.doPrivileged(new PrivilegedAction<JPAFilterAndConverter>() {
         @Override
         public JPAFilterAndConverter run() {
            JPAFilterAndConverter filter = isIndexed && !isCompatMode ? new JPAProtobufFilterAndConverter(jpaQuery) :
                  new JPAFilterAndConverter(jpaQuery, isCompatMode ? CompatibilityReflectionMatcher.class : ProtobufMatcher.class);
            filter.injectDependencies(cache);

            // force early validation!
            filter.getObjectFilter();
            return filter;
         }
      });
   }

   /**
    * Execute Lucene index query.
    */
   private QueryResponse executeIndexedQuery(AdvancedCache<byte[], byte[]> cache, Configuration cacheConfiguration, SerializationContext serCtx, QueryRequest request) throws IOException {
      final SearchManager searchManager = Search.getSearchManager(cache);
      final SearchIntegrator searchFactory = searchManager.unwrap(SearchIntegrator.class);
      final QueryCache queryCache = ComponentRegistryUtils.getQueryCache(cache);  // optional component

      LuceneQueryParsingResult parsingResult;
      Query luceneQuery;

      if (queryCache != null) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(request.getJpqlString(), LuceneQueryParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = parseQuery(cacheConfiguration, serCtx, request.getJpqlString(), searchFactory);
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = parseQuery(cacheConfiguration, serCtx, request.getJpqlString(), searchFactory);
      }

      luceneQuery = parsingResult.getQuery();

      boolean isCompatMode = cacheConfiguration.compatibility().enabled();
      if (!isCompatMode) {
         // restrict on entity type
         BooleanQuery booleanQuery = new BooleanQuery();
         booleanQuery.add(new BooleanClause(new TermQuery(new Term(TYPE_FIELD_NAME, parsingResult.getTargetEntityName())), BooleanClause.Occur.MUST));
         booleanQuery.add(new BooleanClause(luceneQuery, BooleanClause.Occur.MUST));
         luceneQuery = booleanQuery;
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

      return makeResponse(false, serCtx, list, projSize, cacheQuery.getResultSize(), list.size());
   }

   private LuceneQueryParsingResult parseQuery(Configuration cacheConfiguration, final SerializationContext serCtx, String queryString, SearchIntegrator searchFactory) {
      LuceneProcessingChain processingChain;
      if (cacheConfiguration.compatibility().enabled()) {
         EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
            @Override
            public Class<?> getClassFromName(String entityName) {
               return serCtx.canMarshall(entityName) ? serCtx.getMarshaller(entityName).getJavaClass() : null;
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

      return queryParser.parseQuery(queryString, processingChain);
   }

   private FieldDescriptor getFieldDescriptor(Descriptor messageDescriptor, String attributePath) {
      FieldDescriptor fd = null;
      String[] split = attributePath.split("[.]");
      for (int i = 0; i < split.length; i++) {
         String name = split[i];
         fd = messageDescriptor.findFieldByName(name);
         if (fd == null) {
            throw log.unknownField(name, messageDescriptor.getFullName());
         }
         IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null && !indexingMetadata.isFieldIndexed(fd.getNumber())) {
            throw log.fieldIsNotIndexed(name, messageDescriptor.getFullName());
         }
         if (i < split.length - 1) {
            messageDescriptor = fd.getMessageType();
         }
      }
      return fd;
   }

   private QueryResponse makeResponse(boolean isCompatMode, SerializationContext serCtx, List<?> list, int projSize, long totalResults, int numResults) throws IOException {
      List<WrappedMessage> results = new ArrayList<WrappedMessage>(projSize == 0 ? numResults : numResults * projSize);
      for (Object o : list) {
         if (projSize == 0) {
            if (isCompatMode) {
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
      response.setTotalResults(totalResults);
      response.setNumResults(numResults);
      response.setProjectionSize(projSize);
      response.setResults(results);
      return response;
   }
}
