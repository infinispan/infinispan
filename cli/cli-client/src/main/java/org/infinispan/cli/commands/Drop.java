package org.infinispan.cli.commands;

import java.util.Collections;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.completers.CounterCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = Drop.CMD, description = "Drops a cache or a counter", activator = ConnectionActivator.class, groupCommands = {Drop.Cache.class, Drop.Counter.class})
public class Drop extends CliCommand {

   public static final String CMD = "drop";

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      // This command serves only to wrap the sub-commands
      invocation.println(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   @CommandDefinition(name = Cache.CMD, description = "Drop a cache", activator = ConnectionActivator.class)
   public static class Cache extends CliCommand {
      public static final String CMD = "cache";

      @Argument(required = true, completer = CacheCompleter.class, description = "The cache name")
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         if (help) {
            invocation.println(invocation.getHelpInfo());
         }
         CommandInputLine cmd = new CommandInputLine(Drop.CMD)
               .arg("type", Cache.CMD)
               .arg(NAME, name);
         return invocation.execute(cmd);
      }
   }

   @CommandDefinition(name = Counter.CMD, description = "Drop a counter", activator = ConnectionActivator.class)
   public static class Counter extends CliCommand {
      public static final String CMD = "counter";

      @Argument(required = true, completer = CounterCompleter.class, description = "The counter name")
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         if (help) {
            invocation.println(invocation.getHelpInfo());
         }
         CommandInputLine cmd = new CommandInputLine(Drop.CMD)
               .arg("type", Counter.CMD)
               .arg(NAME, name);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }
}
