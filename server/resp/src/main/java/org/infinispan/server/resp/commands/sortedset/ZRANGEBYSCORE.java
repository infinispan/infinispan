package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Returns all the elements in the sorted set at key with a score between min and max
 * (including elements with score equal to min or max).
 * The elements are considered to be ordered from low to high scores.
 *
 * The optional LIMIT argument can be used to only get a range of the matching elements
 * (similar to SELECT LIMIT offset, count in SQL). A negative count returns all elements
 * from the offset.
 *
 * As of Redis version 6.2.0, this command is regarded as deprecated.
 * It can be replaced by {@link ZRANGE}.
 *
 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation</a>
 * @since 15.0
 */
public class ZRANGEBYSCORE extends ZRANGE implements Resp3Command {
   public ZRANGEBYSCORE() {
      super(Set.of(Arg.BYSCORE));
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return super.perform(handler, ctx, arguments);
   }
}
