package org.infinispan.server.resp.commands.sortedset;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * @see <a href="https://redis.io/commands/zcount/">ZCOUNT</a>
 * Returns the number of elements in the sorted set at key with a score between min and max.
 * Min and max can be -inf and +inf, so that you are not required to know the highest or lowest score
 * in the sorted set to get all elements from or up to a certain score.
 *
 * By default, the interval specified by min and max is closed (inclusive).
 * It is possible to specify an open interval (exclusive) by prefixing the score with the character (.
 * min and max can be -inf and +inf, so that you are not required to know the highest or
 * lowest score in the sorted set to get all elements from or up to a certain score.
 * For example:
 * - ZCOUNT people (1 5 Will return all elements with 1 &lt; score &lt;= 5
 * while:
 * - ZCOUNT people (5 (10
 * Will return all the elements with 5 &lt; score &lt; 10 (5 and 10 excluded).
 *
 * @since 15.0
 */
public class ZCOUNT extends RespCommand implements Resp3Command {

   public ZCOUNT() {
      super(4, 1, 1, 1, AclCategory.READ.mask() | AclCategory.SORTEDSET.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] name = arguments.get(0);
      byte[] min = arguments.get(1);
      byte[] max = arguments.get(2);
      ZSetCommonUtils.Score minScore = ZSetCommonUtils.parseScore(min);
      ZSetCommonUtils.Score maxScore = ZSetCommonUtils.parseScore(max);
      if (minScore == null || maxScore == null) {
         handler.writer().minOrMaxNotAValidFloat();
         return handler.myStage();
      }
      if (maxScore.unboundedMin || minScore.unboundedMax) {
         // minScore +inf or maxScore is -inf, return 0 without performing any call
         return handler.stageToReturn(CompletableFuture.completedFuture(0L), ctx, ResponseWriter.INTEGER);
      }

      if (minScore.value == null) {
         minScore.value = Double.MIN_VALUE;
      }

      if (maxScore.value == null) {
         maxScore.value = Double.MAX_VALUE;
      }

      return handler.stageToReturn(handler.getSortedSeMultimap()
            .count(name, minScore.value, minScore.include, maxScore.value, maxScore.include), ctx, ResponseWriter.INTEGER);
   }
}
