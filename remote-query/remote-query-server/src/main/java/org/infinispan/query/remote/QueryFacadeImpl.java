package org.infinispan.query.remote;

import com.google.protobuf.Descriptors;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
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

   public static final String TYPE_FIELD_NAME = "$type";
   public static final String NULL_TOKEN = "_null_";

   @Override
   public byte[] query(AdvancedCache<byte[], byte[]> cache, byte[] query) {
      try {
         return executeQuery(cache, query);
      } catch (IOException e) {
         throw new CacheException("An exception has occurred during query execution", e);
      }
   }

   private byte[] executeQuery(AdvancedCache<byte[], byte[]> cache, byte[] query) throws IOException {
      final SerializationContext serCtx = SerializationContextHolder.getSerializationContext();

      QueryRequest request = ProtobufUtil.fromByteArray(serCtx, query, 0, query.length, QueryRequest.class);

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

      SearchManager searchManager = Search.getSearchManager(cache);
      IspnLuceneProcessingChain processingChain = new IspnLuceneProcessingChain((SearchFactoryIntegrator) searchManager.getSearchFactory(), entityNamesResolver, null);
      QueryParser queryParser = new QueryParser();
      IspnLuceneQueryParsingResult parsingResult = queryParser.parseQuery(request.getJpqlString(), processingChain);

      Sort sort = null;
      if (request.getSortCriteria() != null && !request.getSortCriteria().isEmpty()) {
         SortField[] sortField = new SortField[request.getSortCriteria().size()];
         int i = 0;
         for (QueryRequest.SortCriteria sc : request.getSortCriteria()) {
            //TODO [anistor] sort type is not properly handled right now
            int sortType = SortField.STRING;
            Descriptors.FieldDescriptor field = parsingResult.getTargetType().findFieldByName(sc.getAttributePath());
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
         sort = new Sort(sortField);
      }

      QueryBuilder qb = searchManager.getSearchFactory().buildQueryBuilder().forEntity(parsingResult.getTargetEntity()).get();
      Query q = qb.bool()
            .must(qb.keyword().onField(TYPE_FIELD_NAME).ignoreFieldBridge().matching(parsingResult.getTargetType().getFullName()).createQuery())
            .must(parsingResult.getQuery())
            .createQuery();

      CacheQuery cacheQuery = searchManager.getQuery(q, parsingResult.getTargetEntity());
      if (sort != null) {
         cacheQuery = cacheQuery.sort(sort);
      }
      int projSize = 0;
      if (parsingResult.getProjections() != null && !parsingResult.getProjections().isEmpty()) {
         projSize = parsingResult.getProjections().size();
         cacheQuery.projection(parsingResult.getProjections().toArray(new String[projSize]));
      }
      if (request.getStartOffset() > 0) {
         cacheQuery.firstResult((int) request.getStartOffset());
      }
      if (request.getMaxResults() > 0) {
         cacheQuery.maxResults(request.getMaxResults());
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
}
