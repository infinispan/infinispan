package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CdContextCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Ls.CMD, description = "Lists resources in a path", activator = ConnectionActivator.class)
public class Ls extends CliCommand {
   public static final String CMD = "ls";

   @Argument(description = "The path of the subsystem/item", completer = CdContextCompleter.class)
   String path;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      CommandInputLine cmd = new CommandInputLine(CMD)
            .optionalArg(PATH, path);
      return invocation.execute(cmd);
   }
}
