package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class SizeOperation extends RetryOnFailureOperation<Integer> {

   protected SizeOperation(OperationContext operationContext, CacheOptions options) {
      super(operationContext, SIZE_REQUEST, SIZE_RESPONSE, options, null);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(ByteBufUtil.readVInt(buf));
   }
}
