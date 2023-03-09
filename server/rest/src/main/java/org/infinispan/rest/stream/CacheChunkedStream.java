package org.infinispan.rest.stream;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.reactivex.rxjava3.subscribers.DefaultSubscriber;
import org.reactivestreams.Publisher;

public abstract class CacheChunkedStream<T> {
   protected static final Log logger = LogFactory.getLog(CacheChunkedStream.class);

   // Netty default value with `ChunkedStream`.
   private static final int CHUNK_SIZE = 8192;

   protected final Publisher<T> publisher;

   public CacheChunkedStream(Publisher<T> publisher) {
      this.publisher = publisher;
   }

   public static byte[] readContentAsBytes(Object content) {
      if (content instanceof byte[]) return (byte[]) content;
      if (content instanceof WrappedByteArray) {
         return ((WrappedByteArray) content).getBytes();
      }

      return content.toString().getBytes(StandardCharsets.UTF_8);
   }

   public abstract void subscribe(ChannelHandlerContext ctx);

   abstract static class ByteBufSubscriber<T> extends DefaultSubscriber<T> {
      protected final ChannelHandlerContext ctx;
      protected final ByteBufAllocator allocator;

      protected final GenericFutureListener<Future<Void>> ERROR_LISTENER = f -> {
         try {
            f.get();
         } catch (Throwable t) {
            onError(t);
         }
      };

      private boolean firstEntry = true;

      private ByteBuf pendingBuffer;

      protected ByteBufSubscriber(ChannelHandlerContext ctx, ByteBufAllocator allocator) {
         this.ctx = Objects.requireNonNull(ctx);
         this.allocator = Objects.requireNonNull(allocator);
      }

      protected ByteBuf newByteBuf() {
         // Note this buffer will expand as necessary, but we allocate it with an extra 12.5% buffer to prevent resizes
         // when we reach the chunk size
         return allocator.buffer(CHUNK_SIZE + CHUNK_SIZE >> 3);
      }

      @Override
      protected void onStart() {
         ByteBuf buf = newByteBuf();
         buf.writeByte('[');
         pendingBuffer = buf;
         request(1);
      }

      @Override
      public void onNext(T item) {
         ByteBuf pendingBuf = pendingBuffer;
         if (!firstEntry) {
            pendingBuf.writeByte(',');
         } else {
            firstEntry = false;
         }
         writeItem(item, pendingBuf);
         // Buffer has surpassed our chunk size send it and reallocate
         if (pendingBuf.writerIndex() > CHUNK_SIZE) {
            writeToContext(pendingBuf, false).addListener(f -> {
               try {
                  f.get();
                  request(1);
               } catch (Throwable t) {
                  onError(t);
               }
            });
            pendingBuffer = newByteBuf();
         } else {
            assert pendingBuf.writableBytes() > 0;
            request(1);
         }
      }

      // Implementation can writ the bytes for the given item into the provided ByteBuf. It is recommended to first
      // call ByteBuf.ensureWritable(int) with the amount of bytes required to be written to avoid multiple resizes
      abstract void writeItem(T item, ByteBuf pending);

      @Override
      public void onError(Throwable t) {
         logger.error("Error encountered while streaming cache chunk", t);
         if (pendingBuffer != null) {
            pendingBuffer.release();
            pendingBuffer = null;
         }
         cancel();
         ctx.close();
      }

      @Override
      public void onComplete() {
         ByteBuf buf = pendingBuffer;
         buf.writeByte(']');
         writeToContext(buf, true)
               .addListener(ERROR_LISTENER);
         pendingBuffer = null;
      }

      ChannelFuture writeToContext(ByteBuf buf, boolean isComplete) {
         ChannelFuture completeFuture = ctx.write(new DefaultHttpContent(buf));
         if (isComplete) {
            completeFuture = ctx.write(LastHttpContent.EMPTY_LAST_CONTENT);
         }
         ctx.flush();
         return completeFuture;
      }
   }
}
