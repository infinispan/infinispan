package org.infinispan.client.hotrod.impl.protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;

import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PutOutputStream extends OutputStream {
   private static final int BUFFER_SIZE = 8 * 1024;
   private final BiFunction<ByteBuf, Boolean, CompletionStage<Void>> consumer;
   private final ByteBufAllocator alloc;
   private final Semaphore pendingWrites = new Semaphore(2);
   private final OperationDispatcher dispatcher;
   private ByteBuf buf;
   private volatile Throwable throwable;

   public PutOutputStream(BiFunction<ByteBuf, Boolean, CompletionStage<Void>> consumer, ByteBufAllocator alloc, OperationDispatcher dispatcher) {
      this.consumer = consumer;
      this.alloc = alloc;
      this.dispatcher = dispatcher;
   }

   private void alloc() {
      buf = alloc.buffer(BUFFER_SIZE, BUFFER_SIZE);
   }

   private void consume(ByteBuf buf, boolean complete) throws IOException {
      try {
         // Don't let more than 2 pending writes at any time
         pendingWrites.acquire();
      } catch (InterruptedException e) {
         throw new IOException(e);
      }
      var stage = consumer.apply(buf, complete)
            .whenComplete((___, t) -> {
               pendingWrites.release();
               if (t != null) {
                  throwable = t;
               }
            });
      // If complete then we have to wait until the last stage is complete. Note all prior stages are guaranteed
      // to be complete as well since they are ordered via single Channel event loop
      if (complete) {
         dispatcher.await(stage);
      }
   }

   @Override
   public synchronized void write(int b) throws IOException {
      if (throwable != null) {
         throw new IOException(throwable);
      }
      if (buf == null) {
         alloc();
      } else if (!buf.isWritable()) {
         consume(buf, false);
         alloc();
      }
      buf.writeByte(b);
   }

   @Override
   public synchronized void write(byte[] b, int off, int len) throws IOException {
      if (throwable != null) {
         throw new IOException(throwable);
      }
      if (buf == null) {
         handleNullByteBufWrite(b, off, len);
         return;
      }
      int writeableAmount = buf.writableBytes();
      if (len > writeableAmount) {
         buf.writeBytes(b, off, writeableAmount);
         consume(buf, false);
         buf = null;
         handleNullByteBufWrite(b, off + writeableAmount, len - writeableAmount);
      } else {
         buf.writeBytes(b, off, len);
      }
   }

   private void handleNullByteBufWrite(byte[] b, int off, int len) throws IOException {
      int needToWrite = len;
      if (len > BUFFER_SIZE) {
         while (needToWrite > BUFFER_SIZE) {
            alloc();
            buf.writeBytes(b, off + (len - needToWrite), BUFFER_SIZE);
            consume(buf, false);
            needToWrite -= BUFFER_SIZE;
         }
      }
      alloc();
      buf.writeBytes(b, off + (len - needToWrite), needToWrite);
   }

   @Override
   public synchronized void flush() throws IOException {
      flush(false);
   }

   private void flush(boolean complete) throws IOException {
      if (throwable != null) {
         throw new IOException(throwable);
      }
      if (buf != null && buf.isReadable()) {
         ByteBuf buf = this.buf;
         this.buf = null;
         consume(buf, complete);
      }
   }

   @Override
   public void close() throws IOException {
      flush(true);
   }
}
