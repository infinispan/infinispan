package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.BooleanOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CounterCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Add.CMD, description = "Adds/subtracts a value to/from a counter", activator = ConnectionActivator.class)
public class Add extends CliCommand {
   public static final String CMD = "add";
   public static final String COUNTER = "counter";
   public static final String DELTA = "delta";

   @Argument(completer = CounterCompleter.class, description = "The name of the counter")
   String counter;

   @Option(description = "Does not display the value", completer = BooleanOptionCompleter.class)
   boolean quiet;

   @Option(description = "The delta to add/subtract from/to the value. Defaults to adding 1", defaultValue = "1")
   long delta;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      CommandInputLine cmd = new CommandInputLine(CMD)
            .optionalArg(COUNTER, counter)
            .option(QUIET, quiet)
            .option(DELTA, delta);
      return invocation.execute(cmd);
   }
}
