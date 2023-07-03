package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Returns the score of member in the sorted set at key.
 *
 * If member does not exist in the sorted set, or key does not exist, nil is returned.
 * Bulk string reply: the score of member (a double precision floating point number),
 * represented as string.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zscore/">Redis Documentation</a>
 */
public class ZSCORE extends RespCommand implements Resp3Command {
   public ZSCORE() {
      super(3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] name = arguments.get(0);
      byte[] member = arguments.get(1);
      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();

      CompletionStage<byte[]> score = sortedSetCache.score(name, member)
            .thenApply(r -> r == null ? null : Double.toString(r).getBytes(StandardCharsets.US_ASCII));

      return handler.stageToReturn(score, ctx, Consumers.GET_BICONSUMER);
   }
}
