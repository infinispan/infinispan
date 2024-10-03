package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class NoHotRodOperation<E> extends AbstractHotRodOperation<E> {

   private NoHotRodOperation() {

   }

   private static final NoHotRodOperation<?> INSTANCE;

   static {
      INSTANCE = new NoHotRodOperation<>();
      INSTANCE.complete(null);
   }

   public static <E> NoHotRodOperation<E> instance() {
      return (NoHotRodOperation) INSTANCE;
   }

   @Override
   public E createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      throw new UnsupportedOperationException();
   }

   @Override
   public short requestOpCode() {
      return -1;
   }

   @Override
   public short responseOpCode() {
      return -1;
   }

   @Override
   public int flags() {
      return -1;
   }

   @Override
   public byte[] getCacheNameBytes() {
      return new byte[0];
   }

   @Override
   public String getCacheName() {
      return null;
   }

   @Override
   public DataFormat getDataFormat() {
      return null;
   }
}
