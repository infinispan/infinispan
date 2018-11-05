package org.infinispan.rest;

import static java.nio.charset.StandardCharsets.UTF_8;

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
            return byteBuf.array();
         } else {
            byte[] bufferCopy = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(bufferCopy);
            return bufferCopy;
         }
      }
      return null;
   }

}
