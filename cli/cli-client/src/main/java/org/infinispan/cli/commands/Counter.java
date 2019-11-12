package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CounterCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Counter.CMD, description = "Selects counters", activator = ConnectionActivator.class)
public class Counter extends CliCommand {

   public static final String CMD = "counter";

   @Argument(description = "The name of the counter to select", completer = CounterCompleter.class, required = true)
   String name;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      CommandInputLine cmd = new CommandInputLine(CMD)
            .arg(NAME, name);
      return invocation.execute(cmd);
   }
}
