package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.SortedSetAddArgs;
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
 * Increments the score of member in the sorted set stored at key by increment.
 * If member does not exist in the sorted set, it is added with increment as its score
 * (as if its previous score was 0.0).
 * If key does not exist, a new sorted set with the specified member as its sole member is created.
 *
 * An error is returned when key exists but does not hold a sorted set.
 *
 * The score value should be the string representation of a numeric value,
 * and accepts double precision floating point numbers.
 * It is possible to provide a negative value to decrement the score.
 *
 * Bulk string reply:
 * the new score of member (a double precision floating point number), represented as string.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zincrby/">Redis Documentation</a>
 */
public class ZINCRBY extends RespCommand implements Resp3Command {
   public ZINCRBY() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                                ChannelHandlerContext ctx,
                                                                List<byte[]> arguments) {
      byte[] name = arguments.get(0);
      double score;
      try {
         score = ArgumentUtils.toDouble(arguments.get(1));
      } catch (Exception ex) {
         RespErrorUtil.valueNotAValidFloat(handler.allocator());
         return handler.myStage();
      }
      byte[] value = arguments.get(2);

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      return handler.stageToReturn(sortedSetCache.incrementScore(name, score, value, SortedSetAddArgs.create().incr().build()),
            ctx, Consumers.DOUBLE_BICONSUMER);
   }
}
