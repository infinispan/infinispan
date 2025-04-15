package org.infinispan.server.resp.commands.list;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * RPOPLPUSH
 *
 * @see <a href="https://redis.io/commands/rpoplpush/">RPOPLPUSH</a>
 * @since 15.0
 */
public class RPOPLPUSH extends LMOVE implements Resp3Command {
   public RPOPLPUSH() {
      super(3, AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      return lmoveAndReturn(handler, ctx, arguments, true);
   }
}
