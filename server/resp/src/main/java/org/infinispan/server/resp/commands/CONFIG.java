package org.infinispan.server.resp.commands;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/config/
 * @since 14.0
 */
public class CONFIG extends RespCommand implements Resp3Command {
   public CONFIG() {
      super(-2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      String getOrSet = new String(arguments.get(0), StandardCharsets.UTF_8);
      String name = new String(arguments.get(1), StandardCharsets.UTF_8);

      if ("GET".equalsIgnoreCase(getOrSet)) {
         if ("appendonly".equalsIgnoreCase(name)) {
            ByteBufferUtils.stringToByteBufAscii("*2\r\n+appendonly\r\n+no\r\n", handler.allocator());
         } else if (name.indexOf('*') != -1 || name.indexOf('?') != -1) {
            ByteBufferUtils.stringToByteBufAscii("-ERR CONFIG blob pattern matching not implemented\r\n", handler.allocator());
         } else {
            ByteBufferUtils.stringToByteBuf("*2\r\n+" + name + "\r\n+\r\n", handler.allocator());
         }
      } else if ("SET".equalsIgnoreCase(getOrSet)) {
         Consumers.OK_BICONSUMER.accept(null, handler.allocator());
      } else {
         ByteBufferUtils.stringToByteBuf("-ERR CONFIG " + getOrSet + " not implemented\r\n", handler.allocator());
      }
      return handler.myStage();
   }
}
