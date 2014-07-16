package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.QueryOperation;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.remote.client.QueryResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteQuery implements Query {

   private final RemoteCacheImpl cache;
   private final SerializationContext serializationContext;

   private final String jpqlString;
   private final long startOffset; //todo can this really be long or it has to be int due to limitations in query module?
   private final int maxResults;

   private List results = null;
   private int totalResults;

   public RemoteQuery(RemoteCacheImpl cache, SerializationContext serializationContext,
                      String jpqlString, long startOffset, int maxResults) {
      this.cache = cache;
      this.serializationContext = serializationContext;
      this.jpqlString = jpqlString;
      this.startOffset = startOffset;
      this.maxResults = maxResults;
   }

   public RemoteCacheImpl getCache() {
      return cache;
   }

   public String getJpqlString() {
      return jpqlString;
   }

   public long getStartOffset() {
      return startOffset;
   }

   public int getMaxResults() {
      return maxResults;
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

   @Override
   public int getResultSize() {
      list();
      return totalResults;
   }

   public SerializationContext getSerializationContext() {
      return serializationContext;
   }
}
