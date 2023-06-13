package org.infinispan.server.resp.commands.list;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;
import org.jgroups.util.CompletableFutures;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/lset/
 *
 * Sets the list element at index to element.
 * An error is returned for out of range indexes.
 *
 * @since 15.0
 */
public class LSET extends RespCommand implements Resp3Command {
   public LSET() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      long index = ArgumentUtils.toLong(arguments.get(1));
      byte[] value = arguments.get(2);

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();

      return CompletionStages.handleAndCompose(listMultimap.set(key, index, value) ,(result, t) -> {
         if (t != null) {
            return handleException(handler, t);
         }

         if (!result) {
            RespErrorUtil.noSuchKey(handler.allocator());
            return handler.myStage();
         }

         return handler.stageToReturn(CompletableFutures.completedNull(), ctx, Consumers.OK_BICONSUMER);
      });
   }
}
