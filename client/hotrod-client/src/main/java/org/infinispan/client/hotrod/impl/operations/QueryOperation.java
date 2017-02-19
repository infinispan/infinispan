package org.infinispan.client.hotrod.impl.operations;

import java.io.IOException;
import java.net.SocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class QueryOperation extends RetryOnFailureOperation<QueryResponse> {

   private final RemoteQuery remoteQuery;

   public QueryOperation(Codec codec, TransportFactory transportFactory, byte[] cacheName, AtomicInteger topologyId,
                         int flags, ClientIntelligence clientIntelligence, RemoteQuery remoteQuery) {
      super(codec, transportFactory, cacheName, topologyId, flags, clientIntelligence);
      this.remoteQuery = remoteQuery;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected QueryResponse executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, QUERY_REQUEST);
      QueryRequest queryRequest = new QueryRequest();
      queryRequest.setQueryString(remoteQuery.getQueryString());
      if (remoteQuery.getStartOffset() > 0) {
         queryRequest.setStartOffset(remoteQuery.getStartOffset());
      }
      if (remoteQuery.getMaxResults() >= 0) {
         queryRequest.setMaxResults(remoteQuery.getMaxResults());
      }
      queryRequest.setNamedParameters(getNamedParameters());

      SerializationContext serCtx = remoteQuery.getSerializationContext();
      byte[] requestBytes;
      try {
         requestBytes = ProtobufUtil.toByteArray(serCtx, queryRequest);
      } catch (IOException e) {
         throw new HotRodClientException(e);
      }
      transport.writeArray(requestBytes);
      transport.flush();

      readHeaderAndValidate(transport, params);
      byte[] responseBytes = transport.readArray();
      try {
         QueryResponse queryResponse = ProtobufUtil.fromByteArray(serCtx, responseBytes, QueryResponse.class);
         return queryResponse;
      } catch (IOException e) {
         throw new HotRodClientException(e);
      }
   }

   private List<QueryRequest.NamedParameter> getNamedParameters() {
      Map<String, Object> namedParameters = remoteQuery.getParameters();
      if (namedParameters == null || namedParameters.isEmpty()) {
         return null;
      }
      List<QueryRequest.NamedParameter> params = new ArrayList<QueryRequest.NamedParameter>(namedParameters.size());
      for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
         Object value = e.getValue();
         // todo [anistor] not the most elegant way of doing conversion
         if (value instanceof Enum) {
            EnumMarshaller encoder = (EnumMarshaller) remoteQuery.getSerializationContext().getMarshaller(value.getClass());
            value = encoder.encode((Enum) value);
         } else if (value instanceof Boolean) {
            value = value.toString();
         } else if (value instanceof Date) {
            value = ((Date) value).getTime();
         } else if (value instanceof Instant) {
            value = ((Instant) value).toEpochMilli();
         }
         params.add(new QueryRequest.NamedParameter(e.getKey(), value));
      }
      return params;
   }
}
