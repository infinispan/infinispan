package org.infinispan.client.hotrod.impl.query;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.QueryOperation;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.remote.client.impl.BaseQueryResponse;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteQuery extends BaseQuery {

   private final RemoteCacheImpl<?, ?> cache;
   private final SerializationContext serializationContext;
   private final IndexedQueryMode indexedQueryMode;

   private List<?> results = null;
   private int totalResults;

   RemoteQuery(QueryFactory queryFactory, RemoteCacheImpl<?, ?> cache, SerializationContext serializationContext,
               String queryString, IndexedQueryMode indexQueryMode) {
      super(queryFactory, queryString);
      this.cache = cache;
      this.serializationContext = serializationContext;
      this.indexedQueryMode = indexQueryMode;
   }

   RemoteQuery(QueryFactory queryFactory, RemoteCacheImpl<?, ?> cache, SerializationContext serializationContext,
               String queryString, Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
      super(queryFactory, queryString, namedParameters, projection, startOffset, maxResults);
      this.cache = cache;
      this.serializationContext = serializationContext;
      this.indexedQueryMode = IndexedQueryMode.FETCH;
   }

   @Override
   public void resetQuery() {
      results = null;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> List<T> list() {
      executeQuery();
      return (List<T>) results;
   }

   @Override
   public int getResultSize() {
      executeQuery();
      return totalResults;
   }

   private void executeQuery() {
      if (results == null) {
         validateNamedParameters();
         QueryOperation op = cache.getOperationsFactory().newQueryOperation(this, cache.getDataFormat());
         BaseQueryResponse response = (BaseQueryResponse) await(op.execute());
         totalResults = (int) response.getTotalResults();
         try {
            results = response.extractResults(serializationContext);
         } catch (IOException e) {
            throw new HotRodClientException(e);
         }
      }
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

   public IndexedQueryMode getIndexedQueryMode() {
      return indexedQueryMode;
   }

   @Override
   public String toString() {
      return "RemoteQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            '}';
   }
}
