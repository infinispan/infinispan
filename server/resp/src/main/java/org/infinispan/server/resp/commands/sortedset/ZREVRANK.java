package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
 * The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
 *
 * The optional WITHSCORE argument supplements the command's reply with the score of the element returned.
 *
 * Use {@link ZRANK} to get the rank of an element with the scores ordered from low to high.
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
 * @see <a href="https://redis.io/commands/zrevrank">Redis Documentation</a>
 * @since 15.0
 */
public class ZREVRANK extends ZRANK {
   public ZREVRANK() {
      super();
      this.isRev = true;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return super.perform(handler, ctx, arguments);
   }
}
