package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import net.jcip.annotations.Immutable;

/**
 * Corresponds to the "ping" operation as defined in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PingOperation extends HotRodOperation<PingOperation.PingResult> implements ChannelOperation {
   private static final Log log = LogFactory.getLog(PingOperation.class);
   private static final boolean trace = log.isTraceEnabled();

   private final boolean releaseChannel;

   public PingOperation(Codec codec, AtomicInteger topologyId, Configuration cfg, byte[] cacheName, ChannelFactory channelFactory, boolean releaseChannel) {
      super(PING_REQUEST, PING_RESPONSE, codec, 0, cfg, cacheName, topologyId, channelFactory);
      this.releaseChannel = releaseChannel;
   }

   @Override
   public void invoke(Channel channel) {
      sendHeaderAndRead(channel);
      if (releaseChannel) {
         releaseChannel(channel);
      }
   }

   @Override
   public void cancel(SocketAddress address, Throwable cause) {
      completeExceptionally(cause);
   }

   @Override
   public CompletableFuture<PingResult> execute() {
      throw new UnsupportedOperationException("Cannot execute directly");
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isSuccess(status)) {
         complete(HotRodConstants.hasCompatibility(status)
               ? PingResult.SUCCESS_WITH_COMPAT
               : PingResult.SUCCESS);
      } else {
         String hexStatus = Integer.toHexString(status);
         if (trace)
            log.tracef("Unknown response status: %s", hexStatus);

         throw new InvalidResponseException(
               "Unexpected response status: " + hexStatus);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (cause instanceof HotRodClientException && cause.getMessage().contains("CacheNotFoundException")) {
         complete(PingResult.CACHE_DOES_NOT_EXIST);
         return;
      } else {
         super.exceptionCaught(ctx, cause);
      }
   }

   public enum PingResult {
      // Success if the ping request was responded correctly
      SUCCESS,
      // Success with compatibility enabled
      SUCCESS_WITH_COMPAT,
      // When the ping request fails due to non-existing cache
      CACHE_DOES_NOT_EXIST,
      // For any other type of failures
      FAIL;

      public boolean isSuccess() {
         return this == SUCCESS || this == SUCCESS_WITH_COMPAT;
      }

      public boolean hasCompatibility() {
         return this == SUCCESS_WITH_COMPAT;
      }
   }
}
