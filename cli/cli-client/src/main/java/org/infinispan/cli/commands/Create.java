package org.infinispan.cli.commands;

import java.util.Collections;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CacheConfigurationCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(CliCommand.class)
@GroupCommandDefinition(name = "create", description = "Creates a cache or a counter", activator = ConnectionActivator.class, groupCommands = {Create.Cache.class, Create.Counter.class})
public class Create extends CliCommand {

   @Override
   public CommandResult exec(ContextAwareCommandInvocation commandInvocation) {
      if (help) {
         commandInvocation.println(commandInvocation.getHelpInfo());
      }
      return CommandResult.SUCCESS;
   }

   @CommandDefinition(name = "cache", description = "Create a cache", activator = ConnectionActivator.class)
   public static class Cache extends CliCommand {
      @Argument(required = true)
      String name;

      @Option(completer = CacheConfigurationCompleter.class)
      String template;

      @Option(completer = FileOptionCompleter.class)
      Resource file;

      @Option(defaultValue = "true")
      boolean permanent;

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         if (template != null && file != null) {
            throw Messages.MSG.mutuallyExclusiveOptions("template", "file");
         }
         if (template == null && file == null) {
            throw Messages.MSG.requiresOneOf("template", "file");
         }
         CommandInputLine cmd = new CommandInputLine("create")
               .arg("type", "cache")
               .arg("name", name)
               .optionalArg("template", template)
               .optionalArg("file", file != null ? file.getAbsolutePath() : null)
               .option("permanent", permanent);
         return invocation.execute(cmd);
      }
   }

   @CommandDefinition(name = "counter", description = "Create a counter", activator = ConnectionActivator.class)
   public static class Counter extends CliCommand {
      @Argument(required = true)
      String name;

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine("create")
               .arg("type", "counter")
               .arg("name", name);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }
}
