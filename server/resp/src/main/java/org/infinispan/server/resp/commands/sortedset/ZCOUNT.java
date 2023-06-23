package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Returns the number of elements in the sorted set at key with a score between min and max.
 * Min and max can be -inf and +inf, so that you are not required to know the highest or lowest score
 * in the sorted set to get all elements from or up to a certain score.
 *
 * By default, the interval specified by min and max is closed (inclusive).
 * It is possible to specify an open interval (exclusive) by prefixing the score with the character (.
 * min and max can be -inf and +inf, so that you are not required to know the highest or
 * lowest score in the sorted set to get all elements from or up to a certain score.
 * For example:
 * - ZCOUNT people (1 5 Will return all elements with 1 < score <= 5
 * while:
 * - ZCOUNT people (5 (10
 * Will return all the elements with 5 < score < 10 (5 and 10 excluded).
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zcount">Redis Documentation</a>
 */
public class ZCOUNT extends RespCommand implements Resp3Command {

   public ZCOUNT() {
      super(4, 1, 1, 1);
   }
   public static byte INCLUDE = ((byte)'(');

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] name = arguments.get(0);
      byte[] min = arguments.get(1);
      byte[] max = arguments.get(2);
      boolean includeMin = true;
      boolean includeMax = true;
      double min_val;
      double max_val;
      try {
         if (ArgumentUtils.isNegativeInf(min)) {
            min_val = Double.MIN_VALUE;
         } else if (min[0] == INCLUDE) {
            includeMin = false;
            min_val = ArgumentUtils.toDouble(min, 1);
         } else {
            min_val = ArgumentUtils.toDouble(min);
         }

         if (ArgumentUtils.isPositiveInf(max)) {
            max_val = Double.MAX_VALUE;
         } else if (max[0] == INCLUDE) {
            includeMax = false;
            max_val = ArgumentUtils.toDouble(max, 1);
         } else {
            max_val = ArgumentUtils.toDouble(max);
         }

      } catch (NumberFormatException ex) {
         RespErrorUtil.customError("min or max is not a float", handler.allocator());
         return handler.myStage();
      }

      return handler.stageToReturn(handler.getSortedSeMultimap()
            .count(name, min_val, includeMin, max_val, includeMax), ctx, Consumers.LONG_BICONSUMER);
   }
}
