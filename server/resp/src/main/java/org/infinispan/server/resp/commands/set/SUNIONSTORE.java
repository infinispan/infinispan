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
 * SUNIONSTORE implementation, see:
 * {@link} https://redis.io/commands/sunionstore/
 *
 * @since 15.0
 */
public class SUNIONSTORE extends RespCommand implements Resp3Command {
   public SUNIONSTORE() {
      super(-3, 1, -1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();

      var destination = arguments.get(0);
      var keys = arguments.subList(1, arguments.size());

      var uniqueKeys = SINTER.getUniqueKeys(handler, keys);
      var allEntries = esc.getAll(uniqueKeys);
      return handler.stageToReturn(
            allEntries.thenCompose(sets -> handler.getEmbeddedSetCache().set(destination, SUNION.union(sets.values()))),
            ctx,
            Consumers.LONG_BICONSUMER);
   }
}
