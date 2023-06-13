package org.infinispan.server.resp.commands.list;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * https://redis.io/commands/lrange/
 *
 * Returns the specified elements of the list stored at key.
 * The offsets start and stop are zero-based indexes,
 * with 0 being the first element of the list (the head of the list),
 * 1 being the next element and so on.
 *
 * These offsets can also be negative numbers indicating offsets starting at the end of the list.
 * For example, -1 is the last element of the list, -2 the penultimate, and so on.
 *
 * Out of range indexes will not produce an error.
 * If start is larger than the end of the list, an empty list is returned.
 * If stop is larger than the actual end of the list, will treat it like the
 * last element of the list.
 * @since 15.0
 */
public class LRANGE extends RespCommand implements Resp3Command {

   public LRANGE() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      int start = ArgumentUtils.toInt(arguments.get(1));
      int stop = ArgumentUtils.toInt(arguments.get(2));

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();

      return CompletionStages.handleAndCompose(listMultimap.subList(key, start, stop) ,(c, t) -> {
         if (t != null) {
            return handleException(handler, t);
         }

         return handler.stageToReturn(CompletableFuture.completedFuture(c == null ? Collections.emptyList() : c), ctx, Consumers.GET_ARRAY_BICONSUMER);
      });
   }
}
