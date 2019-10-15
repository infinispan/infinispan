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
@CommandDefinition(name = Reset.CMD, description = "Resets a counter to its initial value", activator = ConnectionActivator.class)
public class Reset extends CliCommand {
   public static final String CMD = "reset";
   public static final String COUNTER = "counter";

   @Argument(completer = CounterCompleter.class)
   String counter;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      CommandInputLine cmd = new CommandInputLine(CMD)
            .optionalArg(COUNTER, counter);
      return invocation.execute(cmd);
   }
}
