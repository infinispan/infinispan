package org.infinispan.server.resp.commands.scripting;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.scripting.LuaTaskEngine;
import org.infinispan.server.resp.serialization.Resp3Type;

import io.netty.channel.ChannelHandlerContext;

/**
 * SCRIPT EXISTS
 *
 * @see <a href="https://redis.io/docs/latest/commands/script-exists/">SCRIPT EXISTS</a>
 * @since 15.1
 */
public class EXISTS extends RespCommand implements Resp3Command {
   protected EXISTS() {
      super(-3, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.SCRIPTING | AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      new String(arguments.get(1), StandardCharsets.US_ASCII);
      List<String> shas = new ArrayList<>();
      for(int i = 1; i < arguments.size(); i++) {
         shas.add(new String(arguments.get(i), StandardCharsets.US_ASCII));
      }
      try {
         LuaTaskEngine engine = handler.respServer().luaEngine();
         List<Integer> exists = engine.scriptExists(shas);
         handler.writer().array(exists, Resp3Type.INTEGER);
         return handler.myStage();
      } catch (Exception e) {
         handler.writer().customError(e.getMessage()); // FIXME
         return handler.myStage();
      }
   }
}
