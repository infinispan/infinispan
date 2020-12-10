package org.infinispan.cli.commands;

import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.infinispan.cli.Context;
import org.infinispan.cli.completers.ConfigPropertyCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "config", description = "Configuration operations", groupCommands = {Config.Set.class, Config.Get.class})
public class Config extends CliCommand {

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      Context context = invocation.getContext();
      context.getProperties().forEach((k, v) -> invocation.printf("%s=%s\n", k, v));
      return CommandResult.SUCCESS;
   }

   @CommandDefinition(name = "set", description = "Sets a configuration property")
   public static class Set extends CliCommand {

      @Arguments(description = "The property name and value", required = true, completer = ConfigPropertyCompleter.class)
      List<String> args;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         Context context = invocation.getContext();
         switch (args.size()) {
            case 1:
               context.setProperty(args.get(0), null);
               break;
            case 2:
               context.setProperty(args.get(0), args.get(1));
               break;
            default:
               throw Messages.MSG.wrongArgumentCount(args.size());
         }

         context.saveProperties();
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "get", description = "Gets a configuration property")
   public static class Get extends CliCommand {

      @Argument(description = "The name of the property", required = true, completer = ConfigPropertyCompleter.class)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         Context context = invocation.getContext();
         invocation.printf("%s=%s\n", name, context.getProperty(name));
         return CommandResult.SUCCESS;
      }
   }
}
