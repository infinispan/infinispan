package org.infinispan.server.resp.commands.connection;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Commands;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * COMMAND
 *
 * @see <a href="https://redis.io/commands/command/">COMMAND</a>
 * @since 14.0
 */
public class COMMAND extends RespCommand implements Resp3Command {
   public static final String NAME = "COMMAND";
   private static final JavaObjectSerializer<Object> SERIALIZER = (ignore, writer) -> {
      List<RespCommand> commands = Commands.all();
      writer.writeNumericPrefix(RespConstants.ARRAY, commands.size());
      for (RespCommand command : commands) {
         describeCommand(command, writer);
      }
   };

   public COMMAND() {
      super(NAME, -1, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.SLOW | AclCategory.CONNECTION;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (!arguments.isEmpty()) {
         handler.writer().customError("COMMAND does not currently support arguments");
      } else {
         // If we ever support a command that isn't ASCII this will need to change
         handler.writer().write(SERIALIZER);
      }
      return handler.myStage();
   }

   private static void describeCommand(RespCommand command, ResponseWriter writer) {
      // Each command has 10 subsections.
      writer.writeNumericPrefix(RespConstants.ARRAY, 10);
      // Name
      writer.simpleString(command.getName());
      // Arity
      writer.integers(command.getArity());
      // Flags, a set
      writer.emptySet();
      // First key
      writer.integers(command.getFirstKeyPos());
      // Last key
      writer.integers(command.getLastKeyPos());
      // Step
      writer.integers(command.getSteps());

      // Additional command metadata
      // ACL categories
      writer.emptySet();
      // Tips
      writer.emptySet();
      // Key specifications
      writer.emptySet();
      // Subcommands
      writer.emptySet();
   }
}
