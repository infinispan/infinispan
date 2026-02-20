package org.infinispan.server.resp.commands.sortedset;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * ZREVRANGE
 *
 * @see <a href="https://redis.io/commands/zrevrange/">ZREVRANGE</a>
 * @since 15.0
 */
public class ZREVRANGE extends ZRANGE implements Resp3Command {
   public ZREVRANGE() {
      super(-4, Set.of(Arg.REV));
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return super.perform(handler, ctx, arguments);
   }
}
