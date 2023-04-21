package org.infinispan.server.resp.commands.pubsub;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.commands.PubSubResp3Command;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.SubscriberHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/punsubscribe/
 * @since 14.0
 */
public class PUNSUBSCRIBE extends RespCommand implements PubSubResp3Command {
   public PUNSUBSCRIBE() {
      super(-1, 0,0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(SubscriberHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      ByteBufferUtils.stringToByteBuf("-ERR not implemented yet\r\n", handler.allocatorToUse());
      return handler.myStage();
   }
}
