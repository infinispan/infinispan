package org.infinispan.server.resp.commands.scripting;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * SCRIPT DEBUG
 *
 * @see <a href="https://redis.io/commands/script-debug/">SCRIPT DEBUG</a>
 * @since 15.1
 */
public class DEBUG extends RespCommand implements Resp3Command {
   protected DEBUG() {
      super(3, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.SCRIPTING | AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      return handler.myStage();
   }
}
