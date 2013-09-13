package org.infinispan.query.remote;

import com.google.protobuf.Descriptors;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.lucene.LuceneProcessingChain;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.infinispan.query.remote.search.IspnLuceneProcessingChain;
import org.infinispan.query.remote.search.IspnLuceneQueryParsingResult;
import org.infinispan.server.core.QueryFacade;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
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
      if (!cache.getCacheConfiguration().indexing().enabled()) {
         throw new CacheException("Indexing is not enabled for cache " + cache.getName());
      }
      try {
         return executeQuery(cache, query);
      } catch (IOException e) {
         throw new CacheException("An exception has occurred during query execution", e);
      }
   }

   private byte[] executeQuery(AdvancedCache<byte[], byte[]> cache, byte[] query) throws IOException {
      final SerializationContext serCtx = ProtobufMetadataManager.getSerializationContext(cache.getCacheManager());

      QueryRequest request = ProtobufUtil.fromByteArray(serCtx, query, 0, query.length, QueryRequest.class);

      SearchManager searchManager = Search.getSearchManager(cache);
      Query luceneQuery;
      List<String> projections;
      Class targetEntity;
      Descriptors.Descriptor messageDescriptor;

      QueryParser queryParser = new QueryParser();
      if (cache.getCacheConfiguration().compatibility().enabled()) {
         final QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
         EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
            @Override
            public Class<?> getClassFromName(String entityName) {
               MessageMarshaller messageMarshaller = (MessageMarshaller) serCtx.getMarshaller(entityName);
               Class clazz = messageMarshaller.getJavaClass();
               Boolean isIndexed = queryInterceptor.getKnownClasses().get(clazz);
               return isIndexed != null && isIndexed ? clazz : null;
            }
         };

         LuceneProcessingChain processingChain = new LuceneProcessingChain((SearchFactoryIntegrator) searchManager.getSearchFactory(), entityNamesResolver, null);
         LuceneQueryParsingResult parsingResult = queryParser.parseQuery(request.getJpqlString(), processingChain);

         MessageMarshaller messageMarshaller = (MessageMarshaller) serCtx.getMarshaller(parsingResult.getTargetEntity());
         messageDescriptor = serCtx.getMessageDescriptor(messageMarshaller.getTypeName());
         targetEntity = parsingResult.getTargetEntity();
         projections = parsingResult.getProjections();
         luceneQuery = parsingResult.getQuery();
      } else {
         EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
            @Override
            public Class<?> getClassFromName(String entityName) {
               try {
                  //todo [anistor] this just checks if the type is known
                  serCtx.getMessageDescriptor(entityName);
               } catch (Exception e) {
                  return null;
               }
               return ProtobufValueWrapper.class;
            }
         };

         IspnLuceneProcessingChain processingChain = new IspnLuceneProcessingChain(serCtx, (SearchFactoryIntegrator) searchManager.getSearchFactory(), entityNamesResolver, null);
         IspnLuceneQueryParsingResult parsingResult = queryParser.parseQuery(request.getJpqlString(), processingChain);
         messageDescriptor = parsingResult.getTargetType();
         targetEntity = parsingResult.getTargetEntity();
         projections = parsingResult.getProjections();

         QueryBuilder qb = searchManager.getSearchFactory().buildQueryBuilder().forEntity(targetEntity).get();
         luceneQuery = qb.bool()
               .must(qb.keyword().onField(TYPE_FIELD_NAME).ignoreFieldBridge().matching(messageDescriptor.getFullName()).createQuery())
               .must(parsingResult.getQuery())
               .createQuery();
      }

      CacheQuery cacheQuery = searchManager.getQuery(luceneQuery, targetEntity);

      if (request.getSortCriteria() != null && !request.getSortCriteria().isEmpty()) {
         SortField[] sortField = new SortField[request.getSortCriteria().size()];
         int i = 0;
         for (QueryRequest.SortCriteria sc : request.getSortCriteria()) {
            //TODO [anistor] sort type is not properly handled right now
            Descriptors.FieldDescriptor field = getFieldDescriptor(messageDescriptor, sc.getAttributePath());
            int sortType = SortField.STRING;
            if (field != null) {
               switch (field.getJavaType()) {
                  case INT:
                  case BOOLEAN:
                  case ENUM:
                     sortType = SortField.INT;
                     break;
                  case LONG:
                     sortType = SortField.LONG;
                     break;
                  case FLOAT:
                     sortType = SortField.FLOAT;
                     break;
                  case DOUBLE:
                     sortType = SortField.DOUBLE;
                     break;
               }
            }
            sortField[i++] = new SortField(sc.getAttributePath(), sortType, !sc.isAscending());
         }
         cacheQuery = cacheQuery.sort(new Sort(sortField));
      }

      int projSize = 0;
      if (projections != null && !projections.isEmpty()) {
         projSize = projections.size();
         cacheQuery = cacheQuery.projection(projections.toArray(new String[projSize]));
      }
      if (request.getStartOffset() > 0) {
         cacheQuery = cacheQuery.firstResult((int) request.getStartOffset());
      }
      if (request.getMaxResults() > 0) {
         cacheQuery = cacheQuery.maxResults(request.getMaxResults());
      }

      List list = cacheQuery.list();
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
      response.setNumResults(list.size());
      response.setProjectionSize(projSize);
      response.setResults(results);

      return ProtobufUtil.toByteArray(serCtx, response);
   }

   private Descriptors.FieldDescriptor getFieldDescriptor(Descriptors.Descriptor messageDescriptor, String attributePath) {
      return messageDescriptor.findFieldByName(attributePath);
   }
}
