package org.infinispan.query.remote;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
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
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.BaseMatcher;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.syntax.BooleShannonExpansion;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanFilterNormalizer;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.objectfilter.impl.syntax.JPATreePrinter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.embedded.impl.EmbeddedLuceneQuery;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQuery;
import org.infinispan.query.dsl.embedded.impl.EmptyResultQuery;
import org.infinispan.query.dsl.embedded.impl.HibernateSearchIndexedFieldProvider;
import org.infinispan.query.dsl.embedded.impl.HybridQuery;
import org.infinispan.query.dsl.embedded.impl.JPAFilterAndConverter;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;
import org.infinispan.query.remote.filter.JPAProtobufFilterAndConverter;
import org.infinispan.query.remote.indexing.IndexingMetadata;
import org.infinispan.query.remote.indexing.ProtobufIndexedFieldProvider;
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

   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   @Override
   public byte[] query(AdvancedCache<byte[], byte[]> cache, byte[] query) {
      try {
         SerializationContext serCtx = ProtobufMetadataManager.getSerializationContextInternal(cache.getCacheManager());
         QueryRequest request = ProtobufUtil.fromByteArray(serCtx, query, 0, query.length, QueryRequest.class);

         Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache);
         boolean isIndexed = cacheConfiguration.indexing().index().isEnabled();
         boolean isCompatMode = cacheConfiguration.compatibility().enabled();
         Query q = buildQuery(cache, serCtx, request, isIndexed, isCompatMode);
         QueryResponse response = makeResponse(isCompatMode, serCtx, q);
         return ProtobufUtil.toByteArray(serCtx, response);
      } catch (IOException e) {
         throw log.errorExecutingQuery(e);
      }
   }

   private Query buildQuery(AdvancedCache<?, ?> cache, SerializationContext serCtx, QueryRequest request, boolean isIndexed, boolean isCompatMode) {
      if (isIndexed) {
         SearchManager searchManager = Search.getSearchManager(cache);     // this also checks access permissions
         SearchIntegrator searchFactory = searchManager.unwrap(SearchIntegrator.class);

         Class<? extends Matcher> matcherImplClass = isCompatMode ? CompatibilityReflectionMatcher.class : ProtobufMatcher.class;
         BaseMatcher matcher = (BaseMatcher) SecurityActions.getCacheComponentRegistry(cache).getComponent(matcherImplClass);
         FilterParsingResult<?> parsingResult = matcher.parse(request.getJpqlString(), null);
         BooleanExpr normalizedExpr = booleanFilterNormalizer.normalize(parsingResult.getQuery());

         if (normalizedExpr == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery(null, cache, request.getJpqlString(), request.getStartOffset(), request.getMaxResults());
         }

         BooleShannonExpansion.IndexedFieldProvider indexedFieldProvider = isCompatMode ?
               new HibernateSearchIndexedFieldProvider(searchFactory, (Class) parsingResult.getTargetEntityMetadata())
               : new ProtobufIndexedFieldProvider((Descriptor) parsingResult.getTargetEntityMetadata());
         BooleShannonExpansion bse = new BooleShannonExpansion(indexedFieldProvider);
         BooleanExpr expansion = bse.expand(normalizedExpr);

         if (expansion == normalizedExpr) {  // identity comparison is intended here!
            // everything is indexed, so go the Lucene way
            //todo [anistor] we should be able to generate the lucene query ourselves rather than depend on hibernate-hql-lucene to do it
            return buildLuceneQuery(cache, isCompatMode, serCtx, request.getJpqlString(), request.getStartOffset(), request.getMaxResults());
         }

         if (expansion == ConstantBooleanExpr.TRUE) {
            // expansion leads to a full non-indexed query
            return new EmbeddedQuery(null, cache, makeFilter(cache, true, isCompatMode, request.getJpqlString()), request.getStartOffset(), request.getMaxResults());
         }

         String expandedJpaOut = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), expansion, null);
         Query expandedQuery = buildLuceneQuery(cache, isCompatMode, serCtx, expandedJpaOut, -1, -1);
         // results are already in protobuf format due to type converter interceptor even if we are in compat mode ...
         ProtobufMatcher protobufMatcher = SecurityActions.getCacheComponentRegistry(cache).getComponent(ProtobufMatcher.class);
         ObjectFilter objectFilter = protobufMatcher.getObjectFilter(request.getJpqlString());
         return new HybridQuery(null, cache, request.getJpqlString(), objectFilter, request.getStartOffset(), request.getMaxResults(), expandedQuery);
      } else {
         return new EmbeddedQuery(null, cache, makeFilter(cache, false, isCompatMode, request.getJpqlString()), request.getStartOffset(), request.getMaxResults());
      }
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
    * Build a Lucene index query.
    */
   private Query buildLuceneQuery(AdvancedCache<?, ?> cache, boolean isCompatMode, SerializationContext serCtx, String jpqlString, long startOffset, int maxResults) {
      final SearchManager searchManager = Search.getSearchManager(cache);     // this also checks access permissions
      final SearchIntegrator searchFactory = searchManager.unwrap(SearchIntegrator.class);
      final QueryCache queryCache = ComponentRegistryUtils.getQueryCache(cache);  // optional component

      LuceneQueryParsingResult parsingResult;

      if (queryCache != null) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpqlString, LuceneQueryParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = transformJpaToLucene(isCompatMode, serCtx, jpqlString, searchFactory);
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = transformJpaToLucene(isCompatMode, serCtx, jpqlString, searchFactory);
      }

      org.apache.lucene.search.Query luceneQuery = parsingResult.getQuery();

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

      String[] projection = null;
      if (parsingResult.getProjections() != null && !parsingResult.getProjections().isEmpty()) {
         int projSize = parsingResult.getProjections().size();
         projection = parsingResult.getProjections().toArray(new String[projSize]);
         cacheQuery = cacheQuery.projection(projection);
      }
      if (startOffset >= 0) {
         cacheQuery = cacheQuery.firstResult((int) startOffset);
      }
      if (maxResults > 0) {
         cacheQuery = cacheQuery.maxResults(maxResults);
      }

      return new EmbeddedLuceneQuery(null, jpqlString, projection, cacheQuery);
   }

   private LuceneQueryParsingResult transformJpaToLucene(boolean isCompatMode, final SerializationContext serCtx, String jpqlString, SearchIntegrator searchFactory) {
      LuceneProcessingChain processingChain;
      if (isCompatMode) {
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
               FieldDescriptor fd = getFieldDescriptor(serCtx, type, propertyPath);
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

      return queryParser.parseQuery(jpqlString, processingChain);
   }

   private FieldDescriptor getFieldDescriptor(SerializationContext serCtx, String type, String attributePath) {
      Descriptor messageDescriptor = serCtx.getMessageDescriptor(type);
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

   private QueryResponse makeResponse(boolean isCompatMode, SerializationContext serCtx, org.infinispan.query.dsl.Query q) throws IOException {
      if (!(q instanceof EmbeddedQuery)) {
         isCompatMode = false; // compat mode is relevant only for fully non-indexed queries
      }

      List<?> list = q.list();
      int numResults = list.size();
      String[] projection = ((BaseQuery) q).getProjection();
      int projSize = projection != null ? projection.length : 0;
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
      response.setTotalResults(q.getResultSize());
      response.setNumResults(numResults);
      response.setProjectionSize(projSize);
      response.setResults(results);
      return response;
   }
}
