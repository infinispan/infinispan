package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * {@link} https://redis.io/commands/sismember/
 *
 * Returns 1 if element is member of the set stored at key, 0 otherwise
 *
 * @since 15.0
 */
public class SISMEMBER extends RespCommand implements Resp3Command {
   public SISMEMBER() {
      super(3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      var resultStage = esc.contains(arguments.get(0),arguments.get(1)).thenApply(v -> v ? 1L : 0L);
      return handler.stageToReturn(resultStage, ctx, Consumers.LONG_BICONSUMER);
   }
}
