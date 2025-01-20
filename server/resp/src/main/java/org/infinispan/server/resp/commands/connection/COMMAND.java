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
      writer.arrayStart(commands.size());
      for (RespCommand command : commands) {
         writer.arrayNext();
         describeCommand(command, writer);
      }
      writer.arrayEnd();
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
      writer.arrayStart(10);

      // Name
      writer.arrayNext();
      writer.simpleString(command.getName());

      // Arity
      writer.arrayNext();
      writer.integers(command.getArity());

      // Flags, a set
      writer.arrayNext();
      writer.emptySet();

      // First key
      writer.arrayNext();
      writer.integers(command.getFirstKeyPos());

      // Last key
      writer.arrayNext();
      writer.integers(command.getLastKeyPos());

      // Step
      writer.arrayNext();
      writer.integers(command.getSteps());

      // Additional command metadata
      // ACL categories
      writer.arrayNext();
      writer.emptySet();

      // Tips
      writer.arrayNext();
      writer.emptySet();

      // Key specifications
      writer.arrayNext();
      writer.emptySet();

      // Subcommands
      writer.arrayNext();
      writer.emptySet();

      writer.arrayEnd();
   }
}
