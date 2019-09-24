package org.infinispan.cli.commands;

import java.util.Collections;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.completers.CounterCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(CliCommand.class)
@GroupCommandDefinition(name = "drop", description = "Drops a cache or a counter", activator = ConnectionActivator.class, groupCommands = {Drop.Cache.class, Drop.Counter.class})
public class Drop extends CliCommand {

   @Override
   public CommandResult exec(ContextAwareCommandInvocation commandInvocation) {
      if (help) {
         commandInvocation.println(commandInvocation.getHelpInfo());
      }
      return CommandResult.SUCCESS;
   }

   @CommandDefinition(name = "cache", description = "Drop a cache", activator = ConnectionActivator.class)
   public static class Cache extends CliCommand {
      @Argument(required = true, completer = CacheCompleter.class)
      String name;

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         if (help) {
            invocation.println(invocation.getHelpInfo());
         }
         CommandInputLine cmd = new CommandInputLine("drop")
               .arg("type", "cache")
               .arg("name", name);
         return invocation.execute(cmd);
      }
   }

   @CommandDefinition(name = "counter", description = "Drop a counter", activator = ConnectionActivator.class)
   public static class Counter extends CliCommand {
      @Argument(required = true, completer = CounterCompleter.class)
      String name;

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         if (help) {
            invocation.println(invocation.getHelpInfo());
         }
         CommandInputLine cmd = new CommandInputLine("drop")
               .arg("type", "counter")
               .arg("name", name);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }
}
