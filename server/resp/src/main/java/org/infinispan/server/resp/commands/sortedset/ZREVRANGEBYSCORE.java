package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Returns all the elements in the sorted set at key with a score between max and min
 * (including elements with score equal to max or min). In contrary to the default ordering of sorted sets,
 * for this command the elements are considered to be ordered from high to low scores.
 *
 * The elements having the same score are returned in reverse lexicographical order.
 *
 * Apart from the reversed ordering, ZREVRANGEBYSCORE is similar to {@link ZRANGEBYSCORE}.
 * Array reply: list of elements in the specified score range (optionally with their scores).
 *
 * As of Redis version 6.2.0, this command is regarded as deprecated.
 *
 * It can be replaced by {@link ZRANGE}.
 *
 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation</a>
 * @since 15.0
 */
public class ZREVRANGEBYSCORE extends ZRANGE implements Resp3Command {
   public ZREVRANGEBYSCORE() {
      super(Set.of(Arg.BYSCORE, Arg.REV));
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return super.perform(handler, ctx, arguments);
   }
}
