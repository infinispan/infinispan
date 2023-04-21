package org.infinispan.server.resp.commands.connection;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/command/
 * @since 14.0
 */
public class COMMAND extends RespCommand implements Resp3Command {
   public static final String NAME = "COMMAND";

   public COMMAND() {
      super(NAME, -1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (!arguments.isEmpty()) {
         ByteBufferUtils.stringToByteBuf("-ERR COMMAND does not currently support arguments\r\n", handler.allocatorToUse());
      } else {
         StringBuilder commandBuilder = new StringBuilder();
         List<RespCommand> commands = RespCommand.all();
         commandBuilder.append("*");
         commandBuilder.append(commands.size());
         commandBuilder.append("\r\n");
         for (RespCommand command : commands){
            addCommand(commandBuilder, command);
         }
         ByteBufferUtils.stringToByteBuf(commandBuilder.toString(), handler.allocatorToUse());
      }
      return handler.myStage();
   }

   private void addCommand(StringBuilder builder, RespCommand command) {
      builder.append("*6\r\n");
      // Name
      builder.append("$").append(ByteBufUtil.utf8Bytes(command.getName())).append("\r\n").append(command.getName()).append("\r\n");
      // Arity
      builder.append(":").append(command.getArity()).append("\r\n");
      // Flags
      builder.append("*0\r\n");
      // First key
      builder.append(":").append(command.getFirstKeyPos()).append("\r\n");
      // Second key
      builder.append(":").append(command.getLastKeyPos()).append("\r\n");
      // Step
      builder.append(":").append(command.getSteps()).append("\r\n");
   }
}
