package org.infinispan.server.resp.commands.connection;

import static org.infinispan.server.resp.RespConstants.CRLF;

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
         ByteBufferUtils.stringToByteBuf("-ERR COMMAND does not currently support arguments\r\n", handler.allocator());
      } else {
         StringBuilder commandBuilder = new StringBuilder();
         List<RespCommand> commands = Commands.all();
         commandBuilder.append("*");
         commandBuilder.append(commands.size());
         commandBuilder.append(CRLF);
         for (RespCommand command : commands){
            addCommand(commandBuilder, command);
         }
         ByteBufferUtils.stringToByteBuf(commandBuilder.toString(), handler.allocator());
      }
      return handler.myStage();
   }

   private void addCommand(StringBuilder builder, RespCommand command) {
      builder.append("*6\r\n");
      // Name
      builder.append("$").append(ByteBufUtil.utf8Bytes(command.getName())).append(CRLF).append(command.getName()).append(CRLF);
      // Arity
      builder.append(":").append(command.getArity()).append(CRLF);
      // Flags
      builder.append("*0\r\n");
      // First key
      builder.append(":").append(command.getFirstKeyPos()).append(CRLF);
      // Second key
      builder.append(":").append(command.getLastKeyPos()).append(CRLF);
      // Step
      builder.append(":").append(command.getSteps()).append(CRLF);
   }
}
