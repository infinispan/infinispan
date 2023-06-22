package org.infinispan.server.memcached;

import java.util.concurrent.CompletionStage;

import org.infinispan.server.memcached.logging.Header;
import org.infinispan.server.memcached.logging.MemcachedAccessLogging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public abstract class MemcachedResponse {
   private final CompletionStage<?> response;
   private final Throwable failure;
   protected final Header header;
   private GenericFutureListener<? extends Future<? super Void>> listener;
   protected int responseBytes = 0;

   private String errorMessage;

   public MemcachedResponse(CompletionStage<?> response, Header header, GenericFutureListener<? extends Future<? super Void>> listener) {
      this(response, null, header, listener);
   }

   public MemcachedResponse(Throwable failure, Header header) {
      this(null, failure, header, null);
   }

   private MemcachedResponse(CompletionStage<?> response, Throwable failure, Header header, GenericFutureListener<? extends Future<? super Void>> listener) {
      this.response = response;
      this.failure = failure;
      this.header = header;
      this.listener = listener;
   }

   public CompletionStage<?> getResponse() {
      return response;
   }

   public boolean isSuccessful() {
      return failure == null;
   }

   public void writeResponse(Object response, ByteBufPool allocator) {
      if (response != null) {
         if (response instanceof ByteBuf[]) {
            responseBytes = writeResponse((ByteBuf[]) response, allocator);
         } else if (response instanceof byte[]) {
            responseBytes = writeResponse((byte[]) response, allocator);
         } else if (response instanceof CharSequence) {
            responseBytes = writeResponse((CharSequence) response, allocator);
         } else {
            responseBytes = writeResponse((ByteBuf) response, allocator);
         }
      }
   }

   public abstract void writeFailure(Throwable throwable, ByteBufPool allocator);

   public void writeFailure(ByteBufPool allocator) {
      writeFailure(failure, allocator);
   }

   protected void useErrorMessage(String message) {
      this.errorMessage = message;
   }

   public void flushed(ChannelFuture future) {
      if (header != null) {
         if (isSuccessful()) {
            MemcachedAccessLogging.logOK(future, header, responseBytes);
         } else {
            MemcachedAccessLogging.logException(future, header, errorMessage, errorMessage.length());
         }
      }

      if (listener != null) {
         future.addListener(listener);
      }
   }

   private static int writeResponse(ByteBuf[] response, ByteBufPool allocator) {
      int size = 0;
      for (ByteBuf buf : response) {
         size += buf.readableBytes();
      }

      ByteBuf output = allocator.acquire(size);
      for (ByteBuf buf : response) {
         output.writeBytes(buf);
      }

      return size;
   }

   private static int writeResponse(byte[] response, ByteBufPool allocator) {
      int size = response.length;
      ByteBuf output = allocator.acquire(size);
      output.writeBytes(response);
      return size;
   }

   private static int writeResponse(CharSequence response, ByteBufPool allocator) {
      int size = response.length();
      ByteBuf output = allocator.acquire(size);
      ByteBufUtil.writeAscii(output, response);
      return size;
   }

   private static int writeResponse(ByteBuf response, ByteBufPool allocator) {
      int size = response.readableBytes();
      ByteBuf output = allocator.acquire(size);
      output.writeBytes(response);
      return size;
   }
}
