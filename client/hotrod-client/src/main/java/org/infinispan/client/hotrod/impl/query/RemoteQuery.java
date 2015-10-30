package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.QueryOperation;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.remote.client.QueryResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteQuery extends BaseQuery {

   private final RemoteCacheImpl cache;
   private final SerializationContext serializationContext;

   private List results = null;
   private int totalResults;

   RemoteQuery(QueryFactory queryFactory, RemoteCacheImpl cache, SerializationContext serializationContext,
               String jpaQuery, Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
      super(queryFactory, jpaQuery, namedParameters, projection, startOffset, maxResults);
      this.cache = cache;
      this.serializationContext = serializationContext;
   }

   @Override
   public void resetQuery() {
      results = null;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> List<T> list() {
      if (results == null) {
         results = executeQuery();
      }

      return (List<T>) results;
   }

   private List<Object> executeQuery() {
      checkParameters();

      QueryOperation op = cache.getOperationsFactory().newQueryOperation(this);
      QueryResponse response = op.execute();
      totalResults = (int) response.getTotalResults();
      return unwrapResults(response.getProjectionSize(), response.getResults());
   }

   private List<Object> unwrapResults(int projectionSize, List<WrappedMessage> results) {
      List<Object> unwrappedResults;
      if (projectionSize > 0) {
         unwrappedResults = new ArrayList<Object>(results.size() / projectionSize);
         Iterator<WrappedMessage> it = results.iterator();
         while (it.hasNext()) {
            Object[] row = new Object[projectionSize];
            for (int i = 0; i < row.length; i++) {
               row[i] = it.next().getValue();
            }
            unwrappedResults.add(row);
         }
      } else {
         unwrappedResults = new ArrayList<Object>(results.size());
         for (WrappedMessage r : results) {
            try {
               byte[] bytes = (byte[]) r.getValue();
               Object o = ProtobufUtil.fromWrappedByteArray(serializationContext, bytes);
               unwrappedResults.add(o);
            } catch (IOException e) {
               throw new HotRodClientException(e);
            }
         }
      }
      return unwrappedResults;
   }

   private void checkParameters() {
      if (namedParameters != null) {
         for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
            if (e.getValue() == null) {
               throw new IllegalStateException("Query parameter '" + e.getKey() + "' was not set");
            }
         }
      }
   }

   @Override
   public int getResultSize() {
      list();
      return totalResults;
   }

   public SerializationContext getSerializationContext() {
      return serializationContext;
   }

   @Override
   public String toString() {
      return "RemoteQuery{" +
            "jpaQuery=" + jpaQuery +
            ", namedParameters=" + namedParameters +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            '}';
   }
}
