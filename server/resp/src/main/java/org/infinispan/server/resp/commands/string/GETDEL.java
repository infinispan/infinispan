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
 * GETDEL Resp Command
 *
 * Get the value of key and delete the key. This command is similar to GET,
 * except for the fact that it also deletes the key on success (if and only
 * if the key's value type is a string).
 * @link https://redis.io/commands/getdel/
 * @since 15.0
 */
public class GETDEL extends RespCommand implements Resp3Command {
   public GETDEL() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);

      return handler.stageToReturn(handler.cache().removeAsync(keyBytes), ctx, Consumers.GET_BICONSUMER);
   }
}
