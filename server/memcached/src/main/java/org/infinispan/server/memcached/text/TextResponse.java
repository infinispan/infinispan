package org.infinispan.server.memcached.text;

import static org.infinispan.server.memcached.text.TextConstants.CLIENT_ERROR_BAD_FORMAT;
import static org.infinispan.server.memcached.text.TextConstants.CRLF;
import static org.infinispan.server.memcached.text.TextConstants.SERVER_ERROR;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.memcached.MemcachedResponse;
import org.infinispan.server.memcached.logging.Header;
import org.infinispan.server.memcached.logging.MemcachedAccessLogging;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @since 15.0
 **/
public class TextResponse extends MemcachedResponse {

   public TextResponse(ByRef<MemcachedResponse> current, Channel ch) {
      super(current, ch);
   }

   @Override
   protected ChannelFuture writeThrowable(Header header, Throwable throwable) {
      Throwable cause = CompletableFutures.extractException(throwable);
      String error;
      if (cause instanceof IOException || cause instanceof IllegalArgumentException) {
         error = CLIENT_ERROR_BAD_FORMAT + " " + cause.getMessage() + CRLF;
      } else if (cause instanceof UnsupportedOperationException) {
         error = "ERROR" + CRLF;
      } else {
         error = SERVER_ERROR + " " + cause.getMessage() + CRLF;
      }
      ChannelFuture future = ch.writeAndFlush(ByteBufUtil.encodeString(ch.alloc(), CharBuffer.wrap(error), StandardCharsets.US_ASCII));
      if (header != null) {
         MemcachedAccessLogging.logException(future, header, error, error.length());
      }
      return future;
   }
}
