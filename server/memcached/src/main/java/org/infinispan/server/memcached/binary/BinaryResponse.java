package org.infinispan.server.memcached.binary;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.memcached.ByteBufPool;
import org.infinispan.server.memcached.MemcachedResponse;
import org.infinispan.server.memcached.logging.Header;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @since 15.0
 **/
public class BinaryResponse extends MemcachedResponse {

   public BinaryResponse(CompletionStage<?> response, Header header, GenericFutureListener<? extends Future<? super Void>> listener) {
      super(response, header, listener);
   }

   public BinaryResponse(Throwable failure, Header header) {
      super(failure, header);
   }

   @Override
   public void writeFailure(Throwable throwable, ByteBufPool allocator) {
      Throwable cause = CompletableFutures.extractException(throwable);
      useErrorMessage(cause.getMessage());
   }
}
