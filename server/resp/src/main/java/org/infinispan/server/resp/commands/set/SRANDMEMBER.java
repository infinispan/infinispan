package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * SRANDMEMBER implementation, see:
 * @link https://redis.io/commands/srandmember/
 * @since 15.0
 */
public class SRANDMEMBER extends RespCommand implements Resp3Command {
   public SRANDMEMBER() {
      super(-2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      final Long count = (arguments.size() <= 1) ? 1 : ArgumentUtils.toLong(arguments.get(1));
      final byte[] key = arguments.get(0);
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      return handler.stageToReturn(esc.pop(key, count, false), ctx, Consumers.COLLECTION_BULK_BICONSUMER);
   }
}
