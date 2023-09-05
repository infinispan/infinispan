package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * {@link} https://redis.io/commands/sdiffstore/
 * @since 15.0
 */
public class SDIFFSTORE extends RespCommand implements Resp3Command {

   public SDIFFSTORE() {
      super(-3, 1, -1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      // If the diff is with the set itself the result will be an empty set
      // but we need to go through the process to check all the keys are set type
      var destination = arguments.get(0);
      var minuends = arguments.subList(1, arguments.size());
      boolean diffItself = minuends.stream().skip(1)
            .anyMatch(item -> Objects.deepEquals(minuends.get(0), item));
      // Wrapping to exclude duplicate keys
      var uniqueKeys = SINTER.getUniqueKeys(handler, minuends);
      var allEntries = esc.getAll(uniqueKeys);
      return handler.stageToReturn(
            allEntries
                  .thenCompose(bucksMap -> esc.set(destination, SDIFF.diff(minuends.get(0), bucksMap, diffItself))),
            ctx, Consumers.LONG_BICONSUMER);
   }
}
