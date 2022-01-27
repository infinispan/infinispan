package org.infinispan.server.resp;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;

public interface RespRequestHandler {
   default RespRequestHandler handleRequest(ChannelHandlerContext ctx, String type, List<byte[]> arguments) {
      ctx.writeAndFlush(stringToByteBuf("-ERR unknown command\r\n", ctx.alloc()));
      return this;
   }

   default ByteBuf stringToByteBufWithExtra(CharSequence string, ByteBufAllocator allocator, int extraBytes) {
      boolean release = true;
      ByteBuf buffer = allocator.buffer(ByteBufUtil.utf8Bytes(string) + extraBytes);

      try {
         ByteBufUtil.writeUtf8(buffer, string);
         release = false;
      } finally {
         if (release) {
            buffer.release();
         }
      }

      return buffer;
   }

   default ByteBuf stringToByteBuf(CharSequence string, ByteBufAllocator allocator) {
      return stringToByteBufWithExtra(string, allocator,0);
   }
}
