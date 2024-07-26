package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * {@link} https://redis.io/commands/smismember/
 *
 * @since 15.0
 */
public class SMISMEMBER extends RespCommand implements Resp3Command {
   public SMISMEMBER() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      byte[][] ba = arguments.subList(1, arguments.size()).toArray(byte[][]::new);
      CompletionStage<List<Long>> resultStage = esc.mIsMember(arguments.get(0), ba);
      return handler.stageToReturn(resultStage, ctx, Resp3Response.ARRAY_INTEGER);
   }
}
