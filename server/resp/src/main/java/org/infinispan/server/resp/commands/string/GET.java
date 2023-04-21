package org.infinispan.server.resp.commands.string;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * GET Resp Command
 *
 * Get the value of key. If the key does not exist the special value nil is returned.
 * An error is returned if the value stored at key is not a string, because GET only handles string values.
 *
 * @link https://redis.io/commands/get/
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

      return handler.stageToReturn(handler.cache().getAsync(keyBytes), ctx, Consumers.GET_BICONSUMER);
   }
}
