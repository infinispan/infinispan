package org.infinispan.server.resp.tx;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import io.netty.channel.ChannelHandlerContext;

public class TransactionCommand {

   private final RespCommand command;
   private final List<byte[]> arguments;

   TransactionCommand(RespCommand command, List<byte[]> arguments) {
      this.command = command;
      this.arguments = arguments;
   }

   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx) {
      return handler.handleRequest(ctx, command, arguments);
   }
}
