package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.concurrent.CompletableFuture;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Performs a step in the challenge/response authentication operation
 *
 * @since 14.0
 */
public class AuthOperation extends HotRodOperation<byte[]> {

   private final Channel channel;
   private final String saslMechanism;
   private final byte[] response;

   public AuthOperation(OperationContext operationContext, Channel channel, String saslMechanism, byte[] response) {
      super(operationContext, AUTH_REQUEST, AUTH_RESPONSE, CacheOptions.DEFAULT);
      this.channel = channel;
      this.saslMechanism = saslMechanism;
      this.response = response;
   }

   @Override
   public CompletableFuture<byte[]> execute() {
      if (!channel.isActive()) {
         throw HOTROD.channelInactive(channel.remoteAddress(), channel.remoteAddress());
      }

      byte[] saslMechBytes = saslMechanism.getBytes(HOTROD_STRING_CHARSET);

      scheduleRead(channel);
      Codec codec = operationContext.getCodec();
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) +
            ByteBufUtil.estimateArraySize(saslMechBytes) + ByteBufUtil.estimateArraySize(response));

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, saslMechBytes);
      ByteBufUtil.writeArray(buf, response);
      channel.writeAndFlush(buf);

      return this;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      boolean complete = buf.readUnsignedByte() > 0;
      byte[] challenge = ByteBufUtil.readArray(buf);
      complete(complete ? null : challenge);
   }
}
