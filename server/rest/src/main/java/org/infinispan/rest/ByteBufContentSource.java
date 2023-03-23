package org.infinispan.rest;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;

import org.infinispan.rest.framework.ContentSource;

import io.netty.buffer.ByteBuf;

public class ByteBufContentSource implements ContentSource {

   private final ByteBuf byteBuf;

   ByteBufContentSource(ByteBuf byteBuf) {
      this.byteBuf = byteBuf;
   }

   @Override
   public String asString() {
      return byteBuf.toString(UTF_8);
   }

   @Override
   public byte[] rawContent() {
      if (byteBuf != null) {
         if (byteBuf.hasArray()) {
            int offset = byteBuf.arrayOffset();
            int size = byteBuf.readableBytes();
            byte[] underlyingBytes = byteBuf.array();
            if (offset == 0 && underlyingBytes.length == size) {
               return underlyingBytes;
            }
            return Arrays.copyOfRange(underlyingBytes, offset, offset + size);
         } else {
            byte[] bufferCopy = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(bufferCopy);
            return bufferCopy;
         }
      }
      return null;
   }

   @Override
   public int size() {
      return byteBuf.readableBytes();
   }
}
