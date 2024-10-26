package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * GET
 *
 * @see <a href="https://redis.io/commands/get/">GET</a>
 * @since 14.0
 */
public class GET extends RespCommand implements Resp3Command {
   public GET() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);

      CompletableFuture<byte[]> async = handler.cache().getAsync(keyBytes);
      return handler.stageToReturn(async, ctx, Resp3Response.BULK_STRING_BYTES);
   }
}
