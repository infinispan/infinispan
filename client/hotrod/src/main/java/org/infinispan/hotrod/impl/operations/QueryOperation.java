package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.query.RemoteQuery;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * @since 14.0
 */
public final class QueryOperation extends RetryOnFailureOperation<Object> {

   private final RemoteQuery remoteQuery;
   private final QuerySerializer querySerializer;
   private final boolean withHitCount;

   public QueryOperation(OperationContext operationContext,
                         CacheOptions options, RemoteQuery remoteQuery, DataFormat dataFormat, boolean withHitCount) {
      super(operationContext, QUERY_REQUEST, QUERY_RESPONSE, options, dataFormat);
      this.remoteQuery = remoteQuery;
      this.querySerializer = QuerySerializer.findByMediaType(dataFormat.getValueType());
      this.withHitCount = withHitCount;
   }

   @Override
   protected void executeOperation(Channel channel) {
      // marshall and write the request
      byte[] requestBytes = querySerializer.serializeQueryRequest(remoteQuery, null);

      scheduleRead(channel);
      Codec codec = operationContext.getCodec();
      // Here we'll rather just serialize the header + payload length than copying the requestBytes around
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateVIntSize(requestBytes.length));

      codec.writeHeader(buf, header);
      ByteBufUtil.writeVInt(buf, requestBytes.length);
      channel.write(buf);
      channel.writeAndFlush(Unpooled.wrappedBuffer(requestBytes));
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      byte[] responseBytes = ByteBufUtil.readArray(buf);
      //FIXME
      //complete(querySerializer.readQueryResponse(channelFactory.getMarshaller(), queryRequest, responseBytes));
   }
}
