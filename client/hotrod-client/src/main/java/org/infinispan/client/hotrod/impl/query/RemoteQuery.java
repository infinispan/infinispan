package org.infinispan.client.hotrod.impl.query;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.QueryOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.api.query.HitCount;
import org.infinispan.commons.query.BaseQuery;
import org.infinispan.commons.query.TotalHitCount;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.remote.client.impl.BaseQueryResponse;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteQuery<T> extends BaseQuery<T> {

   private static final Log log = LogFactory.getLog(RemoteQuery.class);

   private final InternalRemoteCache<?, ?> cache;
   private final SerializationContext serializationContext;

   RemoteQuery(InternalRemoteCache<?, ?> cache, SerializationContext serializationContext,
               String queryString) {
      super(queryString);
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
      return (QueryResult<T>) awaitQueryResult(executeAsync());
   }

   @Override
   public int executeStatement() {
      return awaitQueryResult(executeStatementAsync());
   }

   @Override
   public CloseableIterator<T> iterator() {
      if (maxResults == -1 && startOffset == 0) {
         log.warnPerfRemoteIterationWithoutPagination(queryString);
      }
      return Closeables.iterator(list().iterator());
   }

   @Override
   public int getResultSize() {
      return awaitQueryResult(executeRemotelyAsync(true)).hitCount();
   }

   @Override
   public CompletionStage<org.infinispan.commons.api.query.QueryResult<T>> executeAsync() {
      return internalExecuteAsync().thenApply(CompletableFutures.identity());
   }

   @Override
   public CompletionStage<Integer> executeStatementAsync() {
      return executeRemotelyAsync(false).thenApply(BaseQueryResponse::hitCount);
   }

   private CompletableFuture<BaseQueryResponse<T>> executeRemotelyAsync(boolean withHitCount) {
      validateNamedParameters();
      QueryOperation<T> op = cache.getOperationsFactory().newQueryOperation(this, withHitCount);
      CompletionStage<BaseQueryResponse<T>> stage = cache.getDispatcher().execute(op);
      return stage.toCompletableFuture();
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

   private <R> List<R> extractResults(BaseQueryResponse<R> response) {
      try {
         return response.extractResults(serializationContext);
      } catch (IOException e) {
         throw new HotRodClientException(e);
      }
   }

   private <R> R awaitQueryResult(CompletionStage<R> rsp) {
      return timeout == -1 ? cache.getDispatcher().await(rsp) : await(rsp.toCompletableFuture(), timeout);
   }

   private CompletionStage<QueryResultAdapter<T>> internalExecuteAsync() {
      return executeRemotelyAsync(true).thenApply(QueryResultAdapter<T>::new);
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

   private class QueryResultAdapter<R> implements QueryResult<R> {

      private final BaseQueryResponse<R> queryResponse;

      QueryResultAdapter(BaseQueryResponse<R> queryResponse) {
         this.queryResponse = queryResponse;
      }

      @Override
      public HitCount count() {
         return new TotalHitCount(queryResponse.hitCount(), queryResponse.hitCountExact());
      }

      @Override
      public List<R> list() {
         return extractResults(queryResponse);
      }
   }
}
