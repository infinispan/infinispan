package org.infinispan.server.resp.commands.generic;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.Util;
import org.infinispan.server.resp.commands.Resp3Command;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * <a href="https://redis.io/commands/flushdb/">FLUSHDB</a>
 *
 * @since 15.0
 */
public class FLUSHDB extends RespCommand implements Resp3Command {
   private static final byte[] SYNC_BYTES = "SYNC".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] ASYNC_BYTES = "ASYNC".getBytes(StandardCharsets.US_ASCII);

   public FLUSHDB() {
      super(-1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (arguments.size() == 1) {
         byte[] mode = arguments.get(0);
         if (Util.isAsciiBytesEquals(SYNC_BYTES, mode)) {
            return handler.stageToReturn(handler.cache().clearAsync(), ctx, Consumers.OK_BICONSUMER);
         } else if (Util.isAsciiBytesEquals(ASYNC_BYTES, mode)) {
            handler.cache().clearAsync();
            Consumers.OK_BICONSUMER.accept(null, handler.allocator());
            return handler.myStage();
         } else {
            RespErrorUtil.syntaxError(handler.allocator());
            return handler.myStage();
         }
      }
      return handler.stageToReturn(handler.cache().clearAsync(), ctx, Consumers.OK_BICONSUMER);
   }
}
