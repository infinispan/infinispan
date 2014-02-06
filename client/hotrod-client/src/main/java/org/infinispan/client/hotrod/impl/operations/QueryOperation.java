package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.impl.SortCriteria;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class QueryOperation extends RetryOnFailureOperation<QueryResponse> {

   private final RemoteQuery remoteQuery;

   public QueryOperation(Codec codec, TransportFactory transportFactory, byte[] cacheName, AtomicInteger topologyId,
                         Flag[] flags, RemoteQuery remoteQuery) {
      super(codec, transportFactory, cacheName, topologyId, flags);
      this.remoteQuery = remoteQuery;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers);
   }

   @Override
   protected QueryResponse executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, QUERY_REQUEST);
      QueryRequest queryRequest = new QueryRequest();
      queryRequest.setJpqlString(remoteQuery.getJpqlString());
      queryRequest.setStartOffset(remoteQuery.getStartOffset());
      queryRequest.setMaxResults(remoteQuery.getMaxResults());
      if (remoteQuery.getSortCriteria() != null && !remoteQuery.getSortCriteria().isEmpty()) {
         List<QueryRequest.SortCriteria> scl = new ArrayList<QueryRequest.SortCriteria>();
         for (SortCriteria sc : remoteQuery.getSortCriteria()) {
            QueryRequest.SortCriteria sc2 = new QueryRequest.SortCriteria();
            sc2.setAttributePath(sc.getAttributePath());
            sc2.setAscending(sc.getSortOrder() == SortOrder.ASC);
            scl.add(sc2);
         }
         queryRequest.setSortCriteria(scl);
      }
      SerializationContext serCtx = remoteQuery.getSerializationContext();
      byte[] requestBytes;
      try {
         requestBytes = ProtobufUtil.toByteArray(serCtx, queryRequest);
      } catch (IOException e) {
         throw new CacheException(e);  //todo [anistor] need better exception handling
      }
      transport.writeArray(requestBytes);
      transport.flush();

      readHeaderAndValidate(transport, params);
      byte[] responseBytes = transport.readArray();
      try {
         QueryResponse queryResponse = ProtobufUtil.fromByteArray(serCtx, responseBytes, QueryResponse.class);
         return queryResponse;
      } catch (IOException e) {
         throw new CacheException(e);  //todo [anistor] need better exception handling
      }
   }
}
