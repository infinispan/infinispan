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
 * {@link} https://redis.io/commands/smembers/
 *
 * Returns all the members of the set value stored at key.
 *
 * @since 15.0
 */
public class SMEMBERS extends RespCommand implements Resp3Command {
   public SMEMBERS() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      return handler.stageToReturn(esc.getAsSet(arguments.get(0)), ctx, Consumers.COLLECTION_BULK_BICONSUMER);
   }
}
