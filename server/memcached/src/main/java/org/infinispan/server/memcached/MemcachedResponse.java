package org.infinispan.server.memcached;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.memcached.logging.Header;
import org.infinispan.server.memcached.logging.MemcachedAccessLogging;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public abstract class MemcachedResponse implements BiConsumer<Object, Throwable>, Runnable {
   private volatile Object response;
   private volatile Throwable throwable;
   protected Header header;
   private CompletionStage<Void> responseSent;
   private final ByRef<MemcachedResponse> current;
   protected final Channel ch;
   private GenericFutureListener<? extends Future<? super Void>> listener;

   protected MemcachedResponse(ByRef<MemcachedResponse> current, Channel ch) {
      this.current = current;
      this.ch = ch;
   }

   public void queueResponse(Header header, CompletionStage<?> response) {
      queueResponse(header, response, null);
   }

   public void queueResponse(Header header, CompletionStage<?> operationResponse, GenericFutureListener<? extends Future<? super Void>> listener) {
      assert ch.eventLoop().inEventLoop();
      AggregateCompletionStage<Void> all = CompletionStages.aggregateCompletionStage();
      MemcachedResponse c = current.get();
      if (c != null) {
         all.dependsOn(c.responseSent);
      }
      all.dependsOn(operationResponse.whenComplete(this));
      this.listener = listener;
      this.header = header;
      responseSent = all.freeze()
            .exceptionally(CompletableFutures.toNullFunction())
            .thenRunAsync(this, ch.eventLoop());
      current.set(this);
   }

   @Override
   public void accept(Object response, Throwable throwable) {
      // store the response
      this.response = response;
      this.throwable = throwable;
   }

   @Override
   public void run() {
      ChannelFuture future = throwable != null ? writeThrowable(header, throwable) : writeResponse(header, response);
      if (listener != null) {
         future.addListener(listener);
      }
   }

   protected abstract ChannelFuture writeThrowable(Header header, Throwable throwable);

   protected ChannelFuture writeResponse(Header header, Object response) {
      if (response != null) {
         ChannelFuture future = null;
         int responseBytes = 0;
         if (response instanceof ByteBuf[]) {
            for (ByteBuf buf : (ByteBuf[]) response) {
               responseBytes += buf.readableBytes();
               future = ch.writeAndFlush(buf);
            }
         } else if (response instanceof byte[]) {
            responseBytes = ((byte[]) response).length;
            future = ch.writeAndFlush(ch.alloc().buffer(((byte[]) response).length).writeBytes((byte[]) response));
         } else if (response instanceof CharSequence) {
            responseBytes = ((CharSequence) response).length();
            future = ch.writeAndFlush(ByteBufUtil.writeAscii(ch.alloc(), (CharSequence) response));
         } else {
            responseBytes = ((ByteBuf) response).readableBytes();
            future = ch.writeAndFlush(response);
         }
         if (header != null) {
            MemcachedAccessLogging.logOK(future, header, responseBytes);
         }
         return future;
      } else {
         return null;
      }
   }
}
