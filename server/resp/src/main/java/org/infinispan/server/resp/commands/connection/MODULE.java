package org.infinispan.server.resp.commands.connection;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * <a href="https://redis.io/commands/module-list/">MODULE LIST</a>
 *
 * @since 15.0
 */
public class MODULE extends RespCommand implements Resp3Command {
   public MODULE() {
      super(-1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      handler.checkPermission(AuthorizationPermission.ADMIN);
      String subcommand = new String(arguments.get(0), StandardCharsets.UTF_8).toUpperCase();
      switch (subcommand) {
         case "LIST":
            ByteBufferUtils.stringToByteBufAscii("*0\r\n", handler.allocator());
            break;
         case "LOAD":
         case "LOADEX":
         case "UNLOAD":
            ByteBufferUtils.stringToByteBufAscii("-ERR module loading/unloading unsupported\r\n", handler.allocator());
            break;
      }
      return handler.myStage();
   }
}
