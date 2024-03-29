package org.infinispan.hotrod.impl.protocol;

import java.io.IOException;
import java.util.LinkedList;

import org.infinispan.hotrod.impl.cache.VersionedMetadata;
import org.infinispan.hotrod.impl.transport.netty.ChannelInboundHandlerDefaults;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;

public class ChannelInputStream extends AbstractVersionedInputStream implements ChannelInboundHandlerDefaults {
   public static final String NAME = "stream";

   private final int totalLength;
   private final LinkedList<ByteBuf> bufs = new LinkedList<>();
   private int totalReceived, totalRead;
   private Throwable throwable;

   public ChannelInputStream(VersionedMetadata versionedMetadata, Runnable afterClose, int totalLength) {
      super(versionedMetadata, afterClose);
      this.totalLength = totalLength;
   }

   @Override
   public synchronized int read() throws IOException {
      for (;;) {
         while (bufs.isEmpty()) {
            if (totalRead >= totalLength) {
               return -1;
            }
            try {
               wait();
            } catch (InterruptedException e) {
               IOException ioException = new IOException(e);
               if (throwable != null) {
                  ioException.addSuppressed(throwable);
               }
               throw ioException;
            }
            if (throwable != null) {
               throw new IOException(throwable);
            }
         }
         ByteBuf buf = bufs.peekFirst();
         if (buf.isReadable()) {
            ++totalRead;
            assert totalRead <= totalLength;
            return buf.readUnsignedByte();
         } else {
            buf.release();
            bufs.removeFirst();
         }
      }
   }

   @Override
   public synchronized int read(byte[] b, int off, int len) throws IOException {
      int numRead = 0;
      for (;;) {
         while (numRead == 0 && bufs.isEmpty()) {
            if (totalRead >= totalLength) {
               return -1;
            }
            try {
               wait();
            } catch (InterruptedException e) {
               IOException ioException = new IOException(e);
               if (throwable != null) {
                  ioException.addSuppressed(throwable);
               }
               throw ioException;
            }
            if (throwable != null) {
               throw new IOException(throwable);
            }
         }
         if (bufs.isEmpty()) {
            return numRead;
         }
         ByteBuf buf = bufs.peekFirst();
         int readable = buf.readableBytes();
         if (readable > 0) {
            int prevReaderIndex = buf.readerIndex();
            buf.readBytes(b, off + numRead, Math.min(len - numRead, readable));
            int nowRead = buf.readerIndex() - prevReaderIndex;
            numRead += nowRead;
            totalRead += nowRead;
            assert totalRead <= totalLength : "Now read: " + nowRead + ", read: " + totalRead + ", length" + totalLength;
            if (numRead >= len) {
               return numRead;
            }
         } else {
            buf.release();
            bufs.removeFirst();
         }
      }
   }

   @Override
   public synchronized void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof ByteBuf) {
         ByteBuf buf = (ByteBuf) msg;
         if (totalReceived + buf.readableBytes() <= totalLength) {
            bufs.add(buf);
            totalReceived += buf.readableBytes();
            if (totalReceived == totalLength) {
               ctx.pipeline().remove(this);
            }
         } else if (totalReceived < totalLength) {
            bufs.add(buf.retainedSlice(buf.readerIndex(), totalLength - totalReceived));
            buf.readerIndex(buf.readerIndex() + totalLength - totalReceived);
            totalReceived = totalLength;
            ctx.pipeline().remove(this);
            ctx.fireChannelRead(buf);
         } else {
            ctx.fireChannelRead(buf);
         }
         notifyAll();
      } else {
         ctx.fireChannelRead(msg);
      }
   }

   @Override
   public synchronized void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      this.throwable = cause;
      notifyAll();
   }

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof IdleStateEvent) {
         // swallow the event, this channel is not idle
      } else {
         ctx.fireUserEventTriggered(evt);
      }
   }

   @Override
   public synchronized void close() throws IOException {
      super.close();
      for (ByteBuf buf : bufs) {
         buf.release();
      }
   }

   public boolean moveReadable(ByteBuf buf) {
      if (buf.isReadable()) {
         int numReceived = Math.min(totalLength - totalReceived, buf.readableBytes());
         bufs.add(buf.readBytes(numReceived));
         totalReceived += numReceived;
      }
      return totalReceived < totalLength;
   }
}
