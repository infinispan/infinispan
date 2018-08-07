package org.infinispan.client.hotrod.impl.query;

import static java.util.stream.StreamSupport.stream;
import static org.infinispan.client.hotrod.impl.Util.await;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.QueryOperation;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.remote.client.QueryResponse;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteQuery extends BaseQuery {

   private static final String JSON_TOTAL_RESULTS = "total_results";
   private static final String JSON_HITS = "hits";
   private static final String JSON_HIT = "hit";

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
         Object response = await(op.execute());
         if (response instanceof QueryResponse) {
            QueryResponse resp = (QueryResponse) response;
            totalResults = (int) resp.getTotalResults();
            results = unwrapResults(resp.getProjectionSize(), resp.getResults());
         }
         if (response instanceof JsonObject) {
            JsonObject resp = (JsonObject) response;
            totalResults = resp.get(JSON_TOTAL_RESULTS).getAsInt();
            JsonArray hits = resp.get(JSON_HITS).getAsJsonArray();
            results = stream(hits.spliterator(), false)
                  .map(hit -> hit.getAsJsonObject().get(JSON_HIT).toString())
                  .collect(Collectors.toList());
         }
      }
   }

   private List<Object> unwrapResults(int projectionSize, List<WrappedMessage> results) {
      List<Object> unwrappedResults;
      if (projectionSize > 0) {
         unwrappedResults = new ArrayList<>(results.size() / projectionSize);
         Iterator<WrappedMessage> it = results.iterator();
         while (it.hasNext()) {
            Object[] row = new Object[projectionSize];
            for (int i = 0; i < row.length; i++) {
               row[i] = it.next().getValue();
            }
            unwrappedResults.add(row);
         }
      } else {
         unwrappedResults = new ArrayList<>(results.size());
         for (WrappedMessage r : results) {
            Object o = r.getValue();
            if (serializationContext != null && o instanceof byte[]) {
               try {
                  o = ProtobufUtil.fromWrappedByteArray(serializationContext, (byte[]) o);
               } catch (IOException e) {
                  throw new HotRodClientException(e);
               }
            }
            unwrappedResults.add(o);
         }
      }
      return unwrappedResults;
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
