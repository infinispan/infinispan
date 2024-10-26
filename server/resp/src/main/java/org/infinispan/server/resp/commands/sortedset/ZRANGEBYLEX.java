package org.infinispan.server.resp.commands.sortedset;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * @see <a href="https://redis.io/commands/zrangebylex/">ZRANGEBYLEX</a>
 * Valid start and stop must start with ( or [, in order to specify if the range item
 * is respectively exclusive or inclusive.
 * The special values of + or - for start and stop have the special meaning or positively
 * infinite and negatively infinite strings, so for instance the command ZRANGEBYLEX myzset - +
 * is guaranteed to return all the elements in the sorted set, if all the elements have the same score.
 *
 * As of Redis version 6.2.0, this command is regarded as deprecated.
 *
 * It can be replaced by {@link ZRANGE}.
 * @since 15.0
 */
public class ZRANGEBYLEX extends ZRANGE implements Resp3Command {
   public ZRANGEBYLEX() {
      super(Set.of(Arg.BYLEX));
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return super.perform(handler, ctx, arguments);
   }
}
