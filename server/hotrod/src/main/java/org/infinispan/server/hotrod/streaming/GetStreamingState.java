package org.infinispan.server.hotrod.streaming;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

public class GetStreamingState extends StreamingState {
   private final ByteBuf value;
   private final int batchAmount;
   public GetStreamingState(byte[] key, Channel channelUsed, byte[] value, int batchAmount) {
      super(key, channelUsed);
      this.value = Unpooled.wrappedBuffer(value);
      this.batchAmount = batchAmount;
   }

   @Override
   public ByteBuf nextGet() {
      verifyCorrectThread();
      return value.readRetainedSlice(Math.min(value.readableBytes(), batchAmount));
   }

   @Override
   public boolean isGetComplete() {
      verifyCorrectThread();
      return !value.isReadable();
   }

   @Override
   public void closeGet() {
      verifyCorrectThread();
      close();
   }

   @Override
   public void close() {
      value.release();
   }
}
