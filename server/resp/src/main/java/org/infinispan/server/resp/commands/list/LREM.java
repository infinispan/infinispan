package org.infinispan.server.resp.commands.list;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * LREM
 *
 * @see <a href="https://redis.io/commands/lrem/">LREM</a>
 * @since 15.0
 */
public class LREM extends RespCommand implements Resp3Command {

   public LREM() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      Long count;
      try {
         count = ArgumentUtils.toLong(arguments.get(1));
      } catch (NumberFormatException e) {
         RespErrorUtil.customError("value is not an integer or out of range", handler.allocator());
         return handler.myStage();
      }

      byte[] element = arguments.get(2);

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      return handler.stageToReturn(listMultimap.remove(key, count, element), ctx, Resp3Response.INTEGER);
   }
}
