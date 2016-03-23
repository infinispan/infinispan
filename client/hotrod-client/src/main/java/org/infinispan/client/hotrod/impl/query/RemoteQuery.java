package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.QueryOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
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

   private static final Log log = LogFactory.getLog(RemoteQuery.class, Log.class);

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
         checkParameters();

         QueryOperation op = cache.getOperationsFactory().newQueryOperation(this);
         QueryResponse response = op.execute();
         totalResults = (int) response.getTotalResults();
         results = unwrapResults(response.getProjectionSize(), response.getResults());
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
            if (o instanceof byte[]) {
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

   private void checkParameters() {
      if (namedParameters != null) {
         for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
            if (e.getValue() == null) {
               throw log.queryParameterNotSet(e.getKey());
            }
         }
      }
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
