package org.infinispan.server.resp.commands.connection;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.SubscriberHandler;
import org.infinispan.server.resp.commands.PubSubResp3Command;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * PING
 *
 * @see <a href="https://redis.io/commands/ping/">PING</a>
 * @since 14.0
 */
public class PING extends RespCommand implements Resp3Command, PubSubResp3Command {
   public static final String NAME = "PING";
   private static final byte[] PONG = { 'P', 'O', 'N', 'G'};

   public PING() {
      super(NAME, -1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                                List<byte[]> arguments) {
      if (arguments.isEmpty()) {
         handler.writer().string(PONG);
         return handler.myStage();
      } else if (arguments.size()==1) {
         handler.writer().string(arguments.get(0));
         return handler.myStage();
      }
      return handler.delegate(ctx, this, arguments);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(SubscriberHandler handler, ChannelHandlerContext ctx,
                                                                List<byte[]> arguments) {
      handler.resp3Handler().handleRequest(ctx, this, arguments);
      return handler.myStage();
   }
}
