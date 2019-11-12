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
@CommandDefinition(name = Cas.CMD, description = "Compares and sets counter values", activator = ConnectionActivator.class)
public class Cas extends CliCommand {
   public static final String CMD = "cas";
   public static final String EXPECT = "expect";
   public static final String VALUE = "value";
   public static final String COUNTER = "counter";
   @Argument(completer = CounterCompleter.class)
   String counter;

   @Option(description = "Does not display the value", completer = BooleanOptionCompleter.class)
   boolean quiet;

   @Option(required = true, description = "The expected value")
   long expect;

   @Option(required = true, description = "The new value")
   long value;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      CommandInputLine cmd = new CommandInputLine(CMD)
            .optionalArg(COUNTER, counter)
            .option(QUIET, quiet)
            .option(EXPECT, expect)
            .option(VALUE, value);
      return invocation.execute(cmd);
   }
}
