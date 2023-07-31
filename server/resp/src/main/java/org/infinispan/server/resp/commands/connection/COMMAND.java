package org.infinispan.server.resp.commands.connection;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Commands;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;

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
         ByteBufferUtils.stringToByteBufAscii("-ERR COMMAND does not currently support arguments\r\n", handler.allocator());
      } else {
         StringBuilder commandBuilder = new StringBuilder();
         List<RespCommand> commands = Commands.all();
         commandBuilder.append("*");
         commandBuilder.append(commands.size());
         commandBuilder.append(CRLF_STRING);
         for (RespCommand command : commands){
            addCommand(commandBuilder, command);
         }
         // If we ever support a command that isn't ASCII this will need to change
         ByteBufferUtils.stringToByteBufAscii(commandBuilder.toString(), handler.allocator());
      }
      return handler.myStage();
   }

   private void addCommand(StringBuilder builder, RespCommand command) {
      builder.append("*6\r\n");
      // Name
      builder.append("$").append(ByteBufUtil.utf8Bytes(command.getName())).append(CRLF_STRING).append(command.getName()).append(CRLF_STRING);
      // Arity
      builder.append(":").append(command.getArity()).append(CRLF_STRING);
      // Flags
      builder.append("*0\r\n");
      // First key
      builder.append(":").append(command.getFirstKeyPos()).append(CRLF_STRING);
      // Second key
      builder.append(":").append(command.getLastKeyPos()).append(CRLF_STRING);
      // Step
      builder.append(":").append(command.getSteps()).append(CRLF_STRING);
   }
}
