package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Returns the specified range of elements in the sorted set stored at key.
 * The elements are considered to be ordered from the highest to the lowest score.
 * Descending lexicographical order is used for elements with equal score.
 *
 * Apart from the reversed ordering, ZREVRANGE is similar to ZRANGE.
 *
 * Array reply: list of elements in the specified range (optionally with their scores).
 *
 * Deprecated since 6.2.0, replaced by ZRANGE
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation</a>
 */
public class ZREVRANGE extends ZRANGE implements Resp3Command {
   public ZREVRANGE() {
      super(-3, Set.of(Arg.REV));
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return super.perform(handler, ctx, arguments);
   }
}
