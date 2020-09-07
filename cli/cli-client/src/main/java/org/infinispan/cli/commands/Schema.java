package org.infinispan.cli.commands;

import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Schema.CMD, description = "Manipulates protobuf schemas", activator = ConnectionActivator.class)
public class Schema extends CliCommand {
   public static final String CMD = "schema";
   @Arguments(required = true, description = "The name of the schema")
   List<String> args;

   @Option(completer = FileOptionCompleter.class, shortName = 'u', description = "The protobuf file to upload")
   Resource upload;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      if ((upload != null) && (args.size() != 1)) {
         throw Messages.MSG.illegalCommandArguments();
      } else if ((upload == null) && (args.size() != 2)) {
         throw Messages.MSG.illegalCommandArguments();
      }
      CommandInputLine cmd = new CommandInputLine(Schema.CMD)
            .arg(KEY, args.get(0))
            .optionalArg(VALUE, args.size() > 1 ? args.get(1) : null)
            .optionalArg(FILE, upload != null ? upload.getAbsolutePath() : null);
      return invocation.execute(cmd);
   }
}
