package org.infinispan.client.hotrod.impl.operations;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.impl.BaseQueryResponse;
import org.infinispan.query.remote.client.impl.QueryRequest;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class QueryOperation<T> extends AbstractCacheOperation<BaseQueryResponse<T>> {

   private final RemoteQuery<T> remoteQuery;
   private final QuerySerializer querySerializer;
   private final boolean withHitCount;

   public QueryOperation(InternalRemoteCache<?, ?> remoteCache, RemoteQuery<T> remoteQuery, boolean withHitCount) {
      super(remoteCache);
      this.remoteQuery = remoteQuery;
      this.querySerializer = QuerySerializer.findByMediaType(remoteCache.getDataFormat().getValueType());
      this.withHitCount = withHitCount;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      QueryRequest queryRequest = new QueryRequest();
      queryRequest.setQueryString(remoteQuery.getQueryString());

      if (remoteQuery.getStartOffset() > 0) {
         queryRequest.setStartOffset(remoteQuery.getStartOffset());
      }
      if (remoteQuery.getMaxResults() >= 0) {
         queryRequest.setMaxResults(remoteQuery.getMaxResults());
      }

      if (withHitCount) {
         if (remoteQuery.hitCountAccuracy() != null) {
            queryRequest.hitCountAccuracy(remoteQuery.hitCountAccuracy());
         }
      } else {
         queryRequest.hitCountAccuracy(1);
      }

      queryRequest.setNamedParameters(getNamedParameters());

      queryRequest.setLocal(remoteQuery.isLocal());

      // marshall and write the request
      byte[] requestBytes = querySerializer.serializeQueryRequest(remoteQuery, queryRequest);

      ByteBufUtil.writeArray(buf, requestBytes);
   }

   @Override
   public short requestOpCode() {
      return QUERY_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return QUERY_RESPONSE;
   }

   private List<QueryRequest.NamedParameter> getNamedParameters() {
      Map<String, Object> namedParameters = remoteQuery.getParameters();
      if (namedParameters == null || namedParameters.isEmpty()) {
         return null;
      }
      boolean isProtostream = remoteQuery.getCache().getMarshaller() instanceof ProtoStreamMarshaller;
      SerializationContext serCtx = remoteQuery.getSerializationContext();
      List<QueryRequest.NamedParameter> params = new ArrayList<>(namedParameters.size());
      for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
         Object value = e.getValue();
         // if we're using protobuf, some simple types need conversion
         if (isProtostream) {
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
   public BaseQueryResponse<T> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      byte[] responseBytes = ByteBufUtil.readArray(buf);
      return (BaseQueryResponse<T>) querySerializer.readQueryResponse(
            internalRemoteCache.getMarshaller(), remoteQuery, responseBytes);
   }
}
