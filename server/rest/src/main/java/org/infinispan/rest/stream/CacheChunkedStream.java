package org.infinispan.rest.stream;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Queue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.reactivex.rxjava3.subscribers.DefaultSubscriber;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

public abstract class CacheChunkedStream<T> implements ExtendedChunkedInput<ByteBuf>, CacheStreamProcessor<T> {
   protected static final Log logger = LogFactory.getLog(CacheChunkedStream.class);

   // Netty default value with `ChunkedStream`.
   private static final int CHUNK_SIZE = 8192;

   private final Publisher<T> publisher;
   private final InternalStreamSubscriber subscriber;
   private long offset;

   private volatile boolean stuttered;
   private volatile ChannelHandlerContext ctx;

   public CacheChunkedStream(Publisher<T> publisher) {
      this.publisher = publisher;
      this.subscriber = new InternalStreamSubscriber();
      this.publisher.subscribe(subscriber);
   }

   public static byte[] readContentAsBytes(Object content) {
      if (content instanceof byte[]) return (byte[]) content;
      if (content instanceof WrappedByteArray) {
         return ((WrappedByteArray) content).getBytes();
      }

      return content.toString().getBytes(StandardCharsets.UTF_8);
   }

   @Override
   public void close() { }

   @Override
   public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
      return readChunk(ctx.alloc());
   }

   @Override
   public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
      ByteBuf buf = readChunkOrStutter(allocator);
      stuttered = buf == null;
      return buf;
   }

   private ByteBuf readChunkOrStutter(ByteBufAllocator allocator) throws Exception {
      boolean release = true;
      ByteBuf buffer = allocator.buffer(CHUNK_SIZE);
      try {
         int i = 0;
         while (i < CHUNK_SIZE) {
            // Its possible the buffer was full before reading the element completely,
            // in such case, we do not load another element, we continue from where we left.
            if (!hasElement()) {
               T item = subscriber.peek();
               if (item == null) {
                  // There is the possibility the stream is just empty and completed.
                  // We need to process the `null` until is the end of the input.
                  if (!subscriber.isCompleted() || isEndOfInput()) {
                     break;
                  }
               }

               setCurrent(item);
            }

            byte b;
            while ((b = read()) != -1) {
               buffer.writeByte(b);
               i++;
            }
            subscriber.consume();
         }
         if (i == 0) {
            stuttered = true;
            return null;
         }
         offset += i;
         release = false;
         return buffer;
      } finally {
         if (release) {
            buffer.release();
         }
      }
   }

   @Override
   public final void setContext(ChannelHandlerContext ctx) {
      this.ctx = ctx;
   }

   @Override
   public long length() {
      return -1;
   }

   @Override
   public long progress() {
      return offset;
   }

   private class InternalStreamSubscriber extends DefaultSubscriber<T> {
      private static final int QUEUE_MAX_SIZE = 32;
      private final Queue<T> queue = new ArrayDeque<>(QUEUE_MAX_SIZE);
      private volatile boolean hasNext;

      private InternalStreamSubscriber() {
         this.hasNext = true;
      }

      @Override
      protected void onStart() {
         request(QUEUE_MAX_SIZE);
      }

      @Override
      public void onNext(T item) {
         try {
            queue.add(item);
         } finally {
            // In case we stuttered retuning null, we must notify the ChunkedWriteHandler to resume reading the new value.
            if (stuttered) {
               if (ctx == null) {
                  if (logger.isTraceEnabled()) logger.trace("Chunked request stuttered and can not be resumed");
               } else {
                  ChunkedWriteHandler handler = ctx.pipeline().get(ChunkedWriteHandler.class);
                  if (handler != null) {
                     handler.resumeTransfer();
                  } else if (logger.isTraceEnabled()){
                     logger.trace("Channel handler ChunkedWriteHandler not found to resume after stutter");
                  }
               }
            }
         }
      }

      @Override
      public void onError(Throwable throwable) {
         logger.errorf(throwable, "Failed reading publisher");
         this.hasNext = false;
         // Cancelling might return an incomplete response for the user.
         cancel();
      }

      @Override
      public void onComplete() {
         this.hasNext = false;
      }

      public boolean isCompleted() {
         return !hasNext;
      }

      public T peek() {
         return queue.peek();
      }

      public void consume() {
         if (queue.poll() != null) request(1);
      }
   }
}
