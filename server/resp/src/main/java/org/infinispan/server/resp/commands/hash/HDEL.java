package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>HDEL key field [field ...]</code>` command.
 * <p>
 *    Remove the given fields from the hash stored at key. Unknown fields are just ignored, and a non-existing key
 *    returns 0.
 * </p>
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hdel/">Redis Documentation</a>
 * @author José Bolina
 */
public class HDEL extends RespCommand implements Resp3Command {

   public HDEL() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      if (arguments.size() < 2) {
         RespErrorUtil.wrongArgumentNumber(this, handler.allocator());
         return handler.myStage();
      }

      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      CompletionStage<Integer> cs = multimap.remove(arguments.remove(0), arguments.remove(0), arguments.toArray(new byte[0][]));
      return CompletionStages.handleAndCompose(cs, (res, t) -> {
         if (t != null) {
            return handleException(handler, t);
         }

         return handler.stageToReturn(CompletableFuture.completedFuture(res), ctx, HSET.CONSUMER);
      });
   }
}
