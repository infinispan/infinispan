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
@CommandDefinition(name = Stats.CMD, description = "Shows cache and container statistics", activator = ConnectionActivator.class)
public class Stats extends CliCommand {
   public static final String CMD = "stats";

   @Argument(description = "The path of the resource", completer = CdContextCompleter.class)
   String name;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      CommandInputLine cmd = new CommandInputLine(CMD).optionalArg("name", name);
      return invocation.execute(cmd);
   }
}
