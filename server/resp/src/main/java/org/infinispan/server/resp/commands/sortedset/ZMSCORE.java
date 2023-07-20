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
 * Returns the scores associated with the specified members in the sorted set stored at key.
 *
 * For every member that does not exist in the sorted set, a nil value is returned.
 *
 * Array reply: list of scores or nil associated with the specified member values
 * (a double precision floating point number), represented as strings.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zmscore/">Redis Documentation</a>
 */
public class ZMSCORE extends RespCommand implements Resp3Command {
   public ZMSCORE() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] name = arguments.get(0);
      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      return handler.stageToReturn(sortedSetCache
                  .scores(name, arguments.subList(1, arguments.size())),
            ctx, Consumers.COLLECTION_DOUBLE_BICONSUMER);
   }

}
