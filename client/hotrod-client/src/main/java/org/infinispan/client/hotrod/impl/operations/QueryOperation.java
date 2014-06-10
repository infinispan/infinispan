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
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

import java.io.IOException;
import java.net.SocketAddress;
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
   protected HeaderParams writeRequest(Transport transport) {
      HeaderParams params = writeHeader(transport, QUERY_REQUEST);
      QueryRequest queryRequest = new QueryRequest();
      queryRequest.setJpqlString(remoteQuery.getJpqlString());
      queryRequest.setStartOffset(remoteQuery.getStartOffset());
      queryRequest.setMaxResults(remoteQuery.getMaxResults());

      byte[] requestBytes;
      try {
         requestBytes = ProtobufUtil.toByteArray(remoteQuery.getSerializationContext(), queryRequest);
      } catch (IOException e) {
         throw new CacheException(e);  //todo [anistor] need better exception handling
      }
      transport.writeArray(requestBytes);
      return params;
   }

   @Override
   protected QueryResponse readResponse(Transport transport, HeaderParams params) {
      readHeaderAndValidate(transport, params);
      byte[] responseBytes = transport.readArray();
      try {
         QueryResponse queryResponse = ProtobufUtil.fromByteArray(
               remoteQuery.getSerializationContext(), responseBytes, QueryResponse.class);
         return queryResponse;
      } catch (IOException e) {
         throw new CacheException(e);  //todo [anistor] need better exception handling
      }
   }
}
