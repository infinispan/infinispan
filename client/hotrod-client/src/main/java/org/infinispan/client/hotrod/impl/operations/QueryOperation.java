package org.infinispan.client.hotrod.impl.operations;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class QueryOperation extends RetryOnFailureOperation<QueryResponse> {

   private final RemoteQuery remoteQuery;

   public QueryOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName, AtomicInteger topologyId,
                         int flags, Configuration cfg, RemoteQuery remoteQuery) {
      super(codec, channelFactory, cacheName, topologyId, flags, cfg);
      this.remoteQuery = remoteQuery;
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
      queryRequest.setIndexedQueryMode(remoteQuery.getIndexedQueryMode().toString());

      // marshall and write the request
      byte[] requestBytes;
      final SerializationContext serCtx = remoteQuery.getSerializationContext();
      Marshaller marshaller = null;
      if (serCtx != null) {
         try {
            requestBytes = ProtobufUtil.toByteArray(serCtx, queryRequest);
         } catch (IOException e) {
            throw new HotRodClientException(e);
         }
      } else {
         marshaller = remoteQuery.getCache().getRemoteCacheManager().getMarshaller();
         try {
            requestBytes = marshaller.objectToByteBuffer(queryRequest);
         } catch (IOException e) {
            throw new HotRodClientException(e);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HotRodClientException(e);
         }
      }

      HeaderParams header = headerParams(QUERY_REQUEST);
      scheduleRead(channel, header);

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
   public QueryResponse decodePayload(ByteBuf buf, short status) {
      byte[] responseBytes = ByteBufUtil.readArray(buf);
      SerializationContext serCtx = remoteQuery.getSerializationContext();
      if (serCtx != null) {
         try {
            return ProtobufUtil.fromByteArray(serCtx, responseBytes, QueryResponse.class);
         } catch (IOException e) {
            throw new HotRodClientException(e);
         }
      } else {
         try {
            return (QueryResponse) channelFactory.getMarshaller().objectFromByteBuffer(responseBytes);
         } catch (IOException | ClassNotFoundException e) {
            throw new HotRodClientException(e);
         }
      }
   }
}
