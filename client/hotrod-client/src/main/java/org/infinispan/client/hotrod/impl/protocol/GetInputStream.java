package org.infinispan.client.hotrod.impl.protocol;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.VersionedMetadata;
import org.infinispan.client.hotrod.impl.operations.GetStreamNextResponse;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class GetInputStream extends AbstractVersionedInputStream {
   private final BlockingQueue<ByteBuf> bufs = new ArrayBlockingQueue<>(1);
   private final Supplier<CompletionStage<GetStreamNextResponse>> valueSupplier;
   private final Runnable onClose;
   private ByteBuf currentBuffer;
   private volatile boolean complete;
   private volatile Throwable throwable;

   public GetInputStream(Supplier<CompletionStage<GetStreamNextResponse>> valueSupplier,
                         VersionedMetadata versionedMetadata, ByteBuf initial, boolean complete,
                         Runnable onClose) {
      super(versionedMetadata);
      this.valueSupplier = valueSupplier;
      this.onClose = onClose;
      currentBuffer = initial;
      this.complete = complete;

      if (!complete) {
         sendRequestForMore();
      }
   }

   private void sendRequestForMore() {
      valueSupplier.get()
            .whenComplete((bb, t) -> {
               if (t != null) {
                  throwable = t;
                  bufs.add(Unpooled.EMPTY_BUFFER);
                  return;
               }
               if (bb.complete()) {
                  complete = true;
               }
               bufs.add(bb.value());
            });
   }

   @Override
   public synchronized int read() throws IOException {
      if (throwable != null) {
         throw new IOException(throwable);
      }
      if (currentBuffer != null) {
         if (currentBuffer.isReadable()) {
            return currentBuffer.readUnsignedByte();
         }
         currentBuffer.release();
      }
      if (complete && bufs.isEmpty()) {
         return -1;
      }
      try {
         currentBuffer = retrieveNext();
         return read();
      } catch (InterruptedException e) {
         IOException ioException = new IOException(e);
         if (throwable != null) {
            ioException.addSuppressed(throwable);
         }
         throw ioException;
      }
   }

   @Override
   public synchronized int read(byte[] b, int off, int len) throws IOException {
      if (throwable != null) {
         throw new IOException(throwable);
      }
      int numRead = 0;
      try {
         if (currentBuffer == null) {
            if (complete && bufs.isEmpty()) {
               return -1;
            }
            currentBuffer = retrieveNext();
            if (throwable != null) {
               throw new IOException(throwable);
            }
         }
         if (currentBuffer.isReadable()) {
            int readAmount = Math.min(len, currentBuffer.readableBytes());
            currentBuffer.readBytes(b, off, readAmount);
            if (readAmount == len) {
               return readAmount;
            }
            numRead += readAmount;
         }
         if (!currentBuffer.isReadable()) {
            currentBuffer.release();
            currentBuffer = null;
            if (complete && bufs.isEmpty()) {
               return numRead > 0 ? numRead : -1;
            }
         }

         currentBuffer = bufs.poll();
         if (currentBuffer != null) {
            sendRequestForMore();
            int readAmount = Math.min(len - numRead, currentBuffer.readableBytes());
            currentBuffer.readBytes(b, off, readAmount);
            numRead += readAmount;
         }
         return numRead;
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   private synchronized ByteBuf retrieveNext() throws InterruptedException {
      ByteBuf buf = bufs.take();
      if (!complete && buf != Unpooled.EMPTY_BUFFER) {
         sendRequestForMore();
      }
      return buf;
   }

   @Override
   public void close() throws IOException {
      complete = true;
      ByteBuf buf;
      while ((buf = bufs.poll()) != null) {
         buf.release();
      }

      // Signal to any waiter that we are complete
      bufs.add(Unpooled.EMPTY_BUFFER);
      onClose.run();
   }
}
