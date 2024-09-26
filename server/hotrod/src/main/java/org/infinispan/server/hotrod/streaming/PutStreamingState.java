package org.infinispan.server.hotrod.streaming;

import org.infinispan.metadata.Metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;

public class PutStreamingState extends StreamingState {
   private final Metadata.Builder metadata;
   private final long version;
   CompositeByteBuf compositeByteBuf;
   byte[] value;

   public PutStreamingState(byte[] key, Channel channel,
                            Metadata.Builder metadata, long version) {
      super(key, channel);
      this.metadata = metadata;
      this.version = version;
      compositeByteBuf = channel.alloc().compositeHeapBuffer();
   }

   public byte[] valueForPut() {
      verifyCorrectThread();
      return value;
   }

   @Override
   public Metadata.Builder metadataForPut() {
      verifyCorrectThread();
      return metadata;
   }

   @Override
   public long versionForPut() {
      verifyCorrectThread();
      return version;
   }

   @Override
   public void nextPut(ByteBuf buf) {
      verifyCorrectThread();
      compositeByteBuf.addComponent(true, buf);
   }

   @Override
   public void closePut() {
      verifyCorrectThread();
      close();
   }

   @Override
   public void close() {
      byte[] value = new byte[compositeByteBuf.readableBytes()];
      compositeByteBuf.readBytes(value);
      this.value = value;
      compositeByteBuf.release();
   }
}
