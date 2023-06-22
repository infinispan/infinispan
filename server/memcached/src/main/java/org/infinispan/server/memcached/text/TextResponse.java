package org.infinispan.server.memcached.text;

import static org.infinispan.server.memcached.text.TextConstants.CLIENT_ERROR_BAD_FORMAT;
import static org.infinispan.server.memcached.text.TextConstants.CRLF;
import static org.infinispan.server.memcached.text.TextConstants.SERVER_ERROR;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.memcached.ByteBufPool;
import org.infinispan.server.memcached.MemcachedResponse;
import org.infinispan.server.memcached.logging.Header;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @since 15.0
 **/
public class TextResponse extends MemcachedResponse {

   public TextResponse(CompletionStage<?> response, Header header, GenericFutureListener<? extends Future<? super Void>> listener) {
      super(response, header, listener);
   }

   public TextResponse(Throwable failure, Header header) {
      super(failure, header);
   }

   @Override
   public void writeFailure(Throwable throwable, ByteBufPool allocator) {
      Throwable cause = CompletableFutures.extractException(throwable);
      String error;
      if (cause instanceof IOException || cause instanceof IllegalArgumentException) {
         error = CLIENT_ERROR_BAD_FORMAT + " " + cause.getMessage() + CRLF;
      } else if (cause instanceof UnsupportedOperationException) {
         error = "ERROR" + CRLF;
      } else {
         error = SERVER_ERROR + " " + cause.getMessage() + CRLF;
      }
      useErrorMessage(error);
      responseBytes = error.length();
      ByteBuf output = allocator.acquire(responseBytes);
      ByteBufUtil.writeAscii(output, error);
   }
}
