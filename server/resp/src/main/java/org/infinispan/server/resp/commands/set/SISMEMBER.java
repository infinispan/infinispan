package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * SISMEMBER
 *
 * @see <a href="https://redis.io/commands/sismember/">SISMEMBER</a>
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
      var resultStage = esc.mIsMember(arguments.get(0), arguments.get(1)).thenApply(v -> v.get(0));
      return handler.stageToReturn(resultStage, ctx, ResponseWriter.INTEGER);
   }
}
