package org.infinispan.server.resp.commands.list;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * https://redis.io/commands/rpoplpush/
 * Is like {@link LMOVE} with RIGHT and LEFT
 *
 * @since 15.0
 */
public class RPOPLPUSH extends LMOVE implements Resp3Command {
   public RPOPLPUSH() {
      super(3);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      return lmoveAndReturn(handler, ctx, arguments, true);
   }
}
