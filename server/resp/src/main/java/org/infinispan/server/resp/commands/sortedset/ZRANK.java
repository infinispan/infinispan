package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
 * The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
 *The optional WITHSCORE argument supplements the command's reply with the score of the element returned.
 *
 * Use {@link ZREVRANK} to get the rank of an element with the scores ordered from high to low.
 *
 * If member exists in the sorted set:
 * <ul>
 *    <li>using WITHSCORE, Array reply: an array containing the rank and score of member.</li>
 *    <li> without using WITHSCORE, Integer reply: the rank of member.</li>
 * </ul>
 *
 * If member does not exist in the sorted set or key does not exist:
 * <ul>
 *    <li>using WITHSCORE, Array reply: nil.</li>
 *    <li>without using WITHSCORE, Bulk string reply: nil.</li>
 * </ul>
 *
 * @see <a href="https://redis.io/commands/zrank">Redis Documentation</a>
 * @since 15.0
 */
public class ZRANK  extends RespCommand implements Resp3Command {
   protected boolean isRev;
   public ZRANK() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] name = arguments.get(0);
      byte[] member = arguments.get(1);
      boolean withScore = false;
      if (arguments.size() > 2) {
         withScore = "WITHSCORE".equals(new String(arguments.get(2)));
         if (!withScore) {
            RespErrorUtil.syntaxError(handler.allocator());
            return handler.myStage();
         }
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSet = handler.getSortedSeMultimap();
      if (withScore) {
         return handler.stageToReturn(sortedSet.indexOf(name, member, isRev).thenApply(r -> mapResult(r)), ctx, Consumers.GET_ARRAY_BICONSUMER);
      }
      return handler.stageToReturn(sortedSet.indexOf(name, member, isRev).thenApply(r -> r == null? null : r.getValue()), ctx, Consumers.LONG_BICONSUMER);
   }

   private static Collection<byte[]> mapResult(SortedSetBucket.IndexValue index) {
      if (index == null) {
         return null;
      }
      return List.of(Long.toString(index.getValue()).getBytes(StandardCharsets.US_ASCII),
            Double.toString(index.getScore()).getBytes(StandardCharsets.US_ASCII));
   }
}
