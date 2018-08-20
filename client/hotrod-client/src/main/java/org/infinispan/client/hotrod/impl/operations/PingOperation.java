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
import org.infinispan.commons.dataconversion.MediaType;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import net.jcip.annotations.Immutable;

/**
 * Corresponds to the "ping" operation as defined in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PingOperation extends HotRodOperation<PingOperation.PingResponse> implements ChannelOperation {
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
   public CompletableFuture<PingResponse> execute() {
      throw new UnsupportedOperationException("Cannot execute directly");
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      MediaType keyMediaType = codec.readKeyType(buf);
      MediaType valueMediaType = codec.readKeyType(buf);
      PingOperation.PingResponse pingResponse = new PingOperation.PingResponse(status, keyMediaType, valueMediaType);
      if (pingResponse.isSuccess()) {
         complete(pingResponse);
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
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      PingOperation.PingResponse pingResponse = new PingOperation.PingResponse(cause);
      if (pingResponse.isCacheNotFound()) {
         complete(pingResponse);
      } else {
         super.exceptionCaught(ctx, cause);
      }
   }

   public static class PingResponse {

      public static PingResponse EMPTY = new PingResponse(null);

      private final short status;
      private final MediaType keyMediaType;
      private final MediaType valueMediaType;
      private final Throwable error;

      PingResponse(short status, MediaType keyMediaType, MediaType valueMediaType) {
         this.status = status;
         this.keyMediaType = keyMediaType;
         this.valueMediaType = valueMediaType;
         this.error = null;
      }

      PingResponse(Throwable error) {
         this.status = -1;
         this.keyMediaType = MediaType.APPLICATION_UNKNOWN;
         this.valueMediaType = MediaType.APPLICATION_UNKNOWN;
         this.error = error;
      }

      public short getStatus() {
         return status;
      }

      public boolean isSuccess() {
         return HotRodConstants.isSuccess(status);
      }

      public boolean isObjectStorage() {
         return keyMediaType != null && keyMediaType.match(MediaType.APPLICATION_OBJECT);
      }

      public boolean isFailed() {
         return error != null;
      }

      public boolean isCacheNotFound() {
         return error instanceof HotRodClientException && error.getMessage().contains("CacheNotFoundException");
      }

   }

}
