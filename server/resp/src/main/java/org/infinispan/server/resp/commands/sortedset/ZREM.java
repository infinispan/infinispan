package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Removes the specified members from the sorted set stored at key.
 * Non-existing members are ignored.
 *
 * An error is returned when key exists and does not hold a sorted set.
 * Integer reply:
 * The number of members removed from the sorted set, not including non-existing members.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zrem/">Redis Documentation</a>
 */
public class ZREM extends RespCommand implements Resp3Command {
   public ZREM() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      return handler.stageToReturn(sortedSetCache
                  .removeAll(arguments.get(0), arguments.subList(1, arguments.size())),
            ctx, Consumers.LONG_BICONSUMER);
   }

}
