package org.infinispan.server.resp.commands.generic;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * TIME
 *
 * @see <a href="https://redis.io/commands/time/">time</a>
 * @since 15.0
 */
public class TIME extends RespCommand implements Resp3Command {

   public TIME() {
      super(1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      var now = handler.respServer().getTimeService().instant();
      var nowSec = String.valueOf(now.getEpochSecond()).getBytes(StandardCharsets.US_ASCII);
      var nowMicro = String.valueOf(TimeUnit.NANOSECONDS.toMicros(now.getNano())).getBytes(StandardCharsets.US_ASCII);
      var stage = CompletableFuture.completedFuture(Arrays.asList(nowSec, nowMicro));
      return handler.stageToReturn(stage, ctx, Resp3Response.ARRAY_BULK_STRING);
   }
}
