package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.DecoderException;

/**
 * A fault tolerant ping operation that can survive to node failures.
 *
 * @since 14.0
 */
public class FaultTolerantPingOperation extends RetryOnFailureOperation<PingResponse> {

   protected FaultTolerantPingOperation(OperationContext operationContext, CacheOptions options) {
      super(operationContext, PING_REQUEST, PING_RESPONSE, options, null);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      throw new IllegalStateException("Fault tolerant ping not called manually.");
   }

   @Override
   protected Throwable handleException(Throwable cause, Channel channel, SocketAddress address) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      PingResponse pingResponse = new PingResponse(cause);
      if (pingResponse.isCacheNotFound()) {
         complete(pingResponse);
         return null;
      }
      return super.handleException(cause, channel, address);
   }
}
