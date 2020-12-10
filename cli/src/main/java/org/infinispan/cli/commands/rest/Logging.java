package org.infinispan.cli.commands.rest;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionList;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.LogAppenderCompleter;
import org.infinispan.cli.completers.LogLevelCompleter;
import org.infinispan.cli.completers.LoggersCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "logging", description = "Inspects/Manipulates the server logging configuration", activator = ConnectionActivator.class, groupCommands = {Logging.Loggers.class, Logging.Appenders.class, Logging.Set.class, Logging.Remove.class})
public class Logging extends CliCommand {

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

   @CommandDefinition(name = "list-loggers", description = "Lists available loggers", activator = ConnectionActivator.class)
   public static class Loggers extends RestCliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.server().logging().listLoggers();
      }
   }

   @CommandDefinition(name = "list-appenders", description = "Lists available appenders", activator = ConnectionActivator.class)
   public static class Appenders extends RestCliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.server().logging().listAppenders();
      }
   }

   @CommandDefinition(name = "remove", description = "Removes a logger", activator = ConnectionActivator.class)
   public static class Remove extends RestCliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Argument(required = true, completer = LoggersCompleter.class)
      String name;

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.server().logging().removeLogger(name);
      }
   }

   @CommandDefinition(name = "set", description = "Sets a logger", activator = ConnectionActivator.class)
   public static class Set extends RestCliCommand {

      @Argument(completer = LoggersCompleter.class)
      String name;

      @Option(shortName = 'l', description = "One of OFF, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, ALL", completer = LogLevelCompleter.class)
      String level;

      @OptionList(shortName = 'a', description = "One or more appender names", completer = LogAppenderCompleter.class)
      List<String> appenders;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         String[] appendersArray = appenders != null ? appenders.toArray(new String[0]) : null;
         return client.server().logging().setLogger(name, level, appendersArray);
      }
   }
}
