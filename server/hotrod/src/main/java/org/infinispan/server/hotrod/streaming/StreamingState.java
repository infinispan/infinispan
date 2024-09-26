package org.infinispan.server.hotrod.streaming;

import org.infinispan.metadata.Metadata;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class StreamingState {
   private final byte[] key;
   private final Channel channelUsed;

   public StreamingState(byte[] key, Channel channelUsed) {
      this.key = key;
      this.channelUsed = channelUsed;
   }

   public byte[] getKey() {
      return key;
   }

   public byte[] valueForPut() {
      throw unsupportedType();
   }

   public Metadata.Builder metadataForPut() {
      throw unsupportedType();
   }

   public long versionForPut() {
      throw unsupportedType();
   }

   public void nextPut(ByteBuf buf) {
      throw unsupportedType();
   }

   public ByteBuf nextGet() {
      throw unsupportedType();
   }

   public boolean isGetComplete() {
      throw unsupportedType();
   }

   public void closePut() {
      throw unsupportedType();
   }

   public void closeGet() {
      throw unsupportedType();
   }

   private IllegalStateException unsupportedType() {
      return new IllegalStateException("Unsupported streaming state operation type");
   }

   protected void verifyCorrectThread() {
      if (!channelUsed.eventLoop().inEventLoop()) {
         throw new IllegalStateException("Streaming operation only supported in same event loop it was started!");
      }
   }

   public abstract void close();
}
