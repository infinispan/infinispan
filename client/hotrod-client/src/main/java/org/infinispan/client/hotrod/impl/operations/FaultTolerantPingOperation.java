package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * A fault tolerant ping operation that can survive to node failures.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class FaultTolerantPingOperation extends RetryOnFailureOperation<PingOperation.PingResult> {

   protected FaultTolerantPingOperation(Codec codec, ChannelFactory channelFactory,
                                        byte[] cacheName, AtomicInteger topologyId, int flags,
                                        Configuration cfg) {
      super(codec, channelFactory, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel, HotRodConstants.PING_REQUEST);
   }

   @Override
   public PingOperation.PingResult decodePayload(ByteBuf buf, short status) {
      if (HotRodConstants.isSuccess(status)) {
         return HotRodConstants.hasCompatibility(status)
               ? PingOperation.PingResult.SUCCESS_WITH_COMPAT
               : PingOperation.PingResult.SUCCESS;
      } else {
         String hexStatus = Integer.toHexString(status);
         if (trace)
            log.tracef("Unknown response status: %s", hexStatus);

         throw new InvalidResponseException(
               "Unexpected response status: " + hexStatus);
      }
   }

   @Override
   protected Throwable handleException(Throwable cause, ChannelHandlerContext ctx, SocketAddress address) {
      cause = super.handleException(cause, ctx, address);
      if (cause instanceof HotRodClientException && cause.getMessage().contains("CacheNotFoundException")) {
         complete(PingOperation.PingResult.CACHE_DOES_NOT_EXIST);
         return null;
      }
      return cause;
   }
}
