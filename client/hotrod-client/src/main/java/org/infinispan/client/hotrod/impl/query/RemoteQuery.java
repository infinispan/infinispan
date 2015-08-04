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

      List<Object> results;

      QueryOperation op = cache.getOperationsFactory().newQueryOperation(this);
      QueryResponse response = op.execute();
      totalResults = (int) response.getTotalResults();
      if (response.getProjectionSize() > 0) {
         results = new ArrayList<Object>(response.getResults().size() / response.getProjectionSize());
         Iterator<WrappedMessage> it = response.getResults().iterator();
         while (it.hasNext()) {
            Object[] row = new Object[response.getProjectionSize()];
            for (int i = 0; i < response.getProjectionSize(); i++) {
               row[i] = it.next().getValue();
            }
            results.add(row);
         }
      } else {
         results = new ArrayList<Object>(response.getResults().size());
         for (WrappedMessage r : response.getResults()) {
            try {
               byte[] bytes = (byte[]) r.getValue();
               Object o = ProtobufUtil.fromWrappedByteArray(serializationContext, bytes);
               results.add(o);
            } catch (IOException e) {
               throw new HotRodClientException(e);
            }
         }
      }

      return results;
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
