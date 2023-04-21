package org.infinispan.server.resp.commands;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/info/
 * @since 14.0
 */
public class INFO extends RespCommand implements Resp3Command {
   public INFO() {
      super(-1, 0, 0, 0);
   }


   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                                ChannelHandlerContext ctx,
                                                                List<byte[]> arguments) {
      ByteBufferUtils.stringToByteBuf("-ERR not implemented yet\r\n", handler.allocatorToUse());
      return handler.myStage();
   }
}
