package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.DecoderException;

/**
 * Corresponds to the "ping" operation as defined in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public class PingOperation extends HotRodOperation<PingResponse> implements ChannelOperation {
   private static final Log log = LogFactory.getLog(PingOperation.class);

   private final boolean releaseChannel;

   public PingOperation(OperationContext operationContext, boolean releaseChannel) {
      this(operationContext, PING_REQUEST, PING_RESPONSE, releaseChannel);
   }

   protected PingOperation(OperationContext operationContext, short requestCode, short responseCode, boolean releaseChannel) {
      super(operationContext, requestCode, responseCode, CacheOptions.DEFAULT);
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
      throw new IllegalStateException("Ping response not called manually.");
   }

   @Override
   public void exceptionCaught(Channel channel, Throwable cause) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      PingResponse pingResponse = new PingResponse(cause);
      if (pingResponse.isCacheNotFound()) {
         complete(pingResponse);
      } else {
         super.exceptionCaught(channel, cause);
      }
   }
}
