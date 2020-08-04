package org.infinispan.client.hotrod.impl.query;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.QueryOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.remote.client.impl.BaseQueryResponse;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteQuery<T> extends BaseQuery<T> {

   private static final Log log = LogFactory.getLog(RemoteQuery.class);

   private final InternalRemoteCache<?, ?> cache;
   private final SerializationContext serializationContext;

   RemoteQuery(QueryFactory queryFactory, InternalRemoteCache<?, ?> cache, SerializationContext serializationContext,
               String queryString) {
      super(queryFactory, queryString);
      this.cache = cache;
      this.serializationContext = serializationContext;
   }

   RemoteQuery(QueryFactory queryFactory, InternalRemoteCache<?, ?> cache, SerializationContext serializationContext,
               String queryString, Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
      super(queryFactory, queryString, namedParameters, projection, startOffset, maxResults);
      this.cache = cache;
      this.serializationContext = serializationContext;
   }

   @Override
   public void resetQuery() {
   }

   @Override
   public List<T> list() {
      return execute().list();
   }

   @Override
   public QueryResult<T> execute() {
      BaseQueryResponse<T> response = executeQuery();
      return new QueryResult<T>() {
         @Override
         public OptionalLong hitCount() {
            long totalResults = response.getTotalResults();
            return totalResults == -1 ? OptionalLong.empty() : OptionalLong.of(totalResults);
         }

         @Override
         public List<T> list() {
            try {
               return response.extractResults(serializationContext);
            } catch (IOException e) {
               throw new HotRodClientException(e);
            }
         }
      };
   }

   @Override
   public CloseableIterator<T> iterator() {
      if (maxResults == -1 && startOffset == 0) {
         log.warnPerfRemoteIterationWithoutPagination(queryString);
      }
      return Closeables.iterator(execute().list().iterator());
   }

   @Override
   public int getResultSize() {
      BaseQueryResponse<?> response = executeQuery();
      return (int) response.getTotalResults();
   }

   private BaseQueryResponse<T> executeQuery() {
      validateNamedParameters();
      QueryOperation op = cache.getOperationsFactory().newQueryOperation(this, cache.getDataFormat());
      return (BaseQueryResponse<T>) (timeout != -1 ? await(op.execute(), TimeUnit.NANOSECONDS.toMillis(timeout)) : await(op.execute()));
   }

   /**
    * Get the protobuf SerializationContext or {@code null} if we are not using protobuf.
    */
   public SerializationContext getSerializationContext() {
      return serializationContext;
   }

   public RemoteCache<?, ?> getCache() {
      return cache;
   }

   @Override
   public String toString() {
      return "RemoteQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            '}';
   }
}
