package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * TODO: implement all the command, this is just the start point to help testing ZADD
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zrange">Redis Documentation</a>
 */
public class ZRANGE extends RespCommand implements Resp3Command {

   public ZRANGE() {
      super(-4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      // TODO: implement the command correctly
      byte[] name = arguments.get(0);
      EmbeddedMultimapSortedSetCache sortedSetCache = handler.getSortedSeMultimap();
      CompletionStage<Collection<SortedSetBucket.ScoredValue>> getSortedSet = sortedSetCache.get(name);
      CompletionStage<List<byte[]>> list = getSortedSet
            .thenApply(
            r -> {
               List<byte[]> result = new ArrayList<>();
               r.stream().forEach(e -> {
                  result.add((byte[]) e.getValue());
                  result.add(Double.toString(e.score()).getBytes(StandardCharsets.US_ASCII));
               });
               return result;
            });
      return handler.stageToReturn(list, ctx, Consumers.GET_ARRAY_BICONSUMER);
   }
}
