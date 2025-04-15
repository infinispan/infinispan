package org.infinispan.server.resp.commands.connection;

import static org.infinispan.server.resp.RespUtil.ascii;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Commands;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Type;
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
      super(NAME, -1, 0, 0, 0, AclCategory.SLOW.mask() | AclCategory.CONNECTION.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      ResponseWriter writer = handler.writer();
      if (arguments.isEmpty()) {
         // If we ever support a command that isn't ASCII this will need to change
         writer.write(SERIALIZER);
      } else {
         String subcommand = ascii(arguments.get(0)).toUpperCase();
         switch (subcommand) {
            case "COUNT":
               writer.integers(Commands.all().size());
               break;
            case "INFO":
               if (arguments.size() == 1) {
                  // print them all
                  writer.write(SERIALIZER);
               } else {
                  writer.arrayStart(arguments.size() - 1);
                  for (int i = 1; i < arguments.size(); i++) {
                     RespCommand command = RespCommand.fromString(ascii(arguments.get(i)));
                     if (command == null) {
                        writer.nulls();
                     } else {
                        describeCommand(command, writer);
                     }
                  }
                  writer.arrayEnd();
               }
               break;
            case "LIST":
               if (arguments.size() == 1) {
                  // print them all
                  writer.write(SERIALIZER);
               } else {
                  // TODO: handle FILTERBY MODULE|ACLCAT|PATTERN
                  writer.customError("syntax error");
               }
               break;
            default:
               // this will also catch any unimplemented subcommands such as DOCS, GETKEYS, GETKEYSANDFLAGS
               writer.customError("unknown subcommand '" + subcommand + "'. Try COMMAND HELP.");
               break;
         }
      }
      return handler.myStage();
   }

   private static void describeCommand(RespCommand command, ResponseWriter writer) {
      // Each command has 10 subsections.
      writer.arrayStart(10);

      // Name
      writer.arrayNext();
      writer.string(command.getName());

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
      writer.array(AclCategory.aclNames(command.aclMask()), Resp3Type.BULK_STRING);

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
