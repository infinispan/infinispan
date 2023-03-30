package org.infinispan.server.memcached.binary;

import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.memcached.MemcachedResponse;
import org.infinispan.server.memcached.logging.Header;
import org.infinispan.server.memcached.logging.MemcachedAccessLogging;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @since 15.0
 **/
public class BinaryResponse extends MemcachedResponse {

   public BinaryResponse(ByRef<MemcachedResponse> current, Channel ch) {
      super(current, ch);
   }

   @Override
   protected ChannelFuture writeThrowable(Header header, Throwable throwable) {
      Throwable cause = CompletableFutures.extractException(throwable);
      ch.pipeline().fireExceptionCaught(cause);
      ChannelFuture future = ch.newFailedFuture(cause);
      if (header != null) {
         MemcachedAccessLogging.logException(future, header, cause.getMessage(), 0);
      }
      return future;
   }
}
