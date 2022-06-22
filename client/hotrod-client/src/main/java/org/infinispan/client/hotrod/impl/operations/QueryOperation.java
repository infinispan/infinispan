package org.infinispan.client.hotrod.impl.operations;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.impl.QueryRequest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class QueryOperation extends RetryOnFailureOperation<Object> {

   private final RemoteQuery<?> remoteQuery;
   private final QuerySerializer querySerializer;

   public QueryOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName, AtomicReference<ClientTopology> clientTopology,
                         int flags, Configuration cfg, RemoteQuery<?> remoteQuery, DataFormat dataFormat) {
      super(QUERY_REQUEST, QUERY_RESPONSE, codec, channelFactory, cacheName, clientTopology, flags, cfg, dataFormat, null);
      this.remoteQuery = remoteQuery;
      this.querySerializer = QuerySerializer.findByMediaType(dataFormat.getValueType());
   }

   @Override
   protected void executeOperation(Channel channel) {
      QueryRequest queryRequest = new QueryRequest();
      queryRequest.setQueryString(remoteQuery.getQueryString());
      if (remoteQuery.getStartOffset() > 0) {
         queryRequest.setStartOffset(remoteQuery.getStartOffset());
      }
      if (remoteQuery.getMaxResults() >= 0) {
         queryRequest.setMaxResults(remoteQuery.getMaxResults());
      }
      queryRequest.setNamedParameters(getNamedParameters());

      queryRequest.setLocal(remoteQuery.isLocal());

      // marshall and write the request
      byte[] requestBytes = querySerializer.serializeQueryRequest(remoteQuery, queryRequest);

      scheduleRead(channel);

      // Here we'll rather just serialize the header + payload length than copying the requestBytes around
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateVIntSize(requestBytes.length));

      codec.writeHeader(buf, header);
      ByteBufUtil.writeVInt(buf, requestBytes.length);
      channel.write(buf);
      channel.writeAndFlush(Unpooled.wrappedBuffer(requestBytes));
   }

   private List<QueryRequest.NamedParameter> getNamedParameters() {
      Map<String, Object> namedParameters = remoteQuery.getParameters();
      if (namedParameters == null || namedParameters.isEmpty()) {
         return null;
      }
      final SerializationContext serCtx = remoteQuery.getSerializationContext();
      List<QueryRequest.NamedParameter> params = new ArrayList<>(namedParameters.size());
      for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
         Object value = e.getValue();
         // only if we're using protobuf, some simple types need conversion
         if (serCtx != null) {
            // todo [anistor] not the most elegant way of doing conversion
            if (value instanceof Enum) {
               EnumMarshaller encoder = (EnumMarshaller) serCtx.getMarshaller(value.getClass());
               value = encoder.encode((Enum) value);
            } else if (value instanceof Boolean) {
               value = value.toString();
            } else if (value instanceof Date) {
               value = ((Date) value).getTime();
            } else if (value instanceof Instant) {
               value = ((Instant) value).toEpochMilli();
            }
         }
         params.add(new QueryRequest.NamedParameter(e.getKey(), value));
      }
      return params;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      byte[] responseBytes = ByteBufUtil.readArray(buf);
      complete(querySerializer.readQueryResponse(channelFactory.getMarshaller(), remoteQuery, responseBytes));
   }
}
