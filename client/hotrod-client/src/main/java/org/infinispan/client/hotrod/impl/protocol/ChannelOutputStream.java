package org.infinispan.client.hotrod.impl.protocol;

import java.io.IOException;
import java.io.OutputStream;

import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;

public class ChannelOutputStream extends OutputStream implements GenericFutureListener<Future<? super Void>> {
   private static final int BUFFER_SIZE = 8 * 1024;
   private final Channel channel;
   private final ChannelOutputStreamListener listener;
   private ByteBuf buf;

   public ChannelOutputStream(Channel channel, ChannelOutputStreamListener listener) {
      this.channel = channel;
      this.listener = listener;
   }

   private void alloc() {
      buf = channel.alloc().buffer(BUFFER_SIZE);
   }

   private ChannelPromise writePromise() {
      // When the write fails due to event loop closed we would not be notified
      // if we used the the same event loop as executor for the promise
      ChannelPromise promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
      promise.addListener(this);
      return promise;
   }

   @Override
   public void write(int b) throws IOException {
      if (buf == null) {
         alloc();
      } else if (!buf.isWritable()) {
         channel.write(vIntBuffer(buf.writerIndex()), writePromise());
         channel.write(buf, writePromise());
         alloc();
      }
      buf.writeByte(b);
   }

   private ByteBuf vIntBuffer(int value) {
      ByteBuf buffer = channel.alloc().buffer(ByteBufUtil.estimateVIntSize(value));
      ByteBufUtil.writeVInt(buffer, value);
      return buffer;
   }

   @Override
   public void write(byte[] b, int off, int len) throws IOException {
      if (buf == null) {
         if (len > BUFFER_SIZE) {
            channel.write(vIntBuffer(len), writePromise());
            channel.write(Unpooled.wrappedBuffer(b, off, len), writePromise());
         } else {
            alloc();
            buf.writeBytes(b, off, len);
         }
         return;
      }
      if (len > buf.capacity()) {
         if (buf != null) {
            channel.write(vIntBuffer(buf.writerIndex()), writePromise());
            channel.write(buf, writePromise());
            buf = null;
         }
         channel.write(vIntBuffer(len), writePromise());
         channel.write(Unpooled.wrappedBuffer(b, off, len), writePromise());
         return;
      } else if (len > buf.writableBytes()) {
         int numWritten = buf.writableBytes();
         buf.writeBytes(b, off, numWritten);
         off += numWritten;
         len -= numWritten;
         channel.write(vIntBuffer(buf.writerIndex()), writePromise());
         channel.write(buf, writePromise());
         alloc();
      }
      buf.writeBytes(b, off, len);
   }

   @Override
   public void flush() throws IOException {
      if (buf != null && buf.writerIndex() > 0) {
         channel.write(vIntBuffer(buf.writerIndex()), writePromise());
         channel.writeAndFlush(buf, writePromise());
         buf = null;
      } else {
         channel.flush();
      }
   }

   @Override
   public void close() throws IOException {
      flush();
      ByteBuf terminal = channel.alloc().buffer(1);
      terminal.writeByte(0);
      channel.writeAndFlush(terminal, writePromise());
      listener.onClose(channel);
   }

   @Override
   public void operationComplete(Future<? super Void> future) throws Exception {
      if (!future.isSuccess()) {
         listener.onError(channel, future.cause());
      }
   }
}
