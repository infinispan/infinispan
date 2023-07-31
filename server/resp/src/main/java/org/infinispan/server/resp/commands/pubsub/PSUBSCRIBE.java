package org.infinispan.server.resp.commands.pubsub;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.SubscriberHandler;
import org.infinispan.server.resp.commands.PubSubResp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/psubscribe/
 * @since 14.0
 */
public class PSUBSCRIBE extends RespCommand implements PubSubResp3Command {
   public PSUBSCRIBE() {
      super(-2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(SubscriberHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      ByteBufferUtils.stringToByteBufAscii("-ERR not implemented yet\r\n", handler.allocator());
      return handler.myStage();
   }
}
