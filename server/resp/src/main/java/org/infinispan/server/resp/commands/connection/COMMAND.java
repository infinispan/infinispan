package org.infinispan.server.resp.commands.connection;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Commands;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ByteBufferUtils;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.RespConstants;

import io.netty.channel.ChannelHandlerContext;

/**
 * @see <a href="https://redis.io/commands/command/">Redis documentation</a>
 * @since 14.0
 */
public class COMMAND extends RespCommand implements Resp3Command {
   public static final String NAME = "COMMAND";
   private static final JavaObjectSerializer<Object> SERIALIZER = (ignore, alloc) -> {
      List<RespCommand> commands = Commands.all();
      ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, commands.size(), alloc);
      for (RespCommand command : commands) {
         describeCommand(command, alloc);
      }
   };

   public COMMAND() {
      super(NAME, -1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (!arguments.isEmpty()) {
         RespErrorUtil.customError("COMMAND does not currently support arguments", handler.allocator());
      } else {
         // If we ever support a command that isn't ASCII this will need to change
         Resp3Response.write(handler.allocator(), SERIALIZER);
      }
      return handler.myStage();
   }

   private static void describeCommand(RespCommand command, ByteBufPool alloc) {
      // Each command has 10 subsections.
      ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 10, alloc);
      // Name
      Resp3Response.simpleString(command.getName(), alloc);
      // Arity
      Resp3Response.integers(command.getArity(), alloc);
      // Flags, a set
      Resp3Response.emptySet(alloc);
      // First key
      Resp3Response.integers(command.getFirstKeyPos(), alloc);
      // Last key
      Resp3Response.integers(command.getLastKeyPos(), alloc);
      // Step
      Resp3Response.integers(command.getSteps(), alloc);

      // Additional command metadata
      // ACL categories
      Resp3Response.emptySet(alloc);
      // Tips
      Resp3Response.emptySet(alloc);
      // Key specifications
      Resp3Response.emptySet(alloc);
      // Subcommands
      Resp3Response.emptySet(alloc);
   }
}
