package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.DataSourceCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
@GroupCommandDefinition(name = DataSource.CMD, description = "Performs operations on data sources", activator = ConnectionActivator.class, groupCommands = {DataSource.Ls.class, DataSource.Test.class})
public class DataSource extends CliCommand {

   public static final String CMD = "datasource";

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

   @CommandDefinition(name = "ls", description = "Lists data sources", activator = ConnectionActivator.class)
   public static class Ls extends RestCliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.server().dataSourceNames();
      }
   }

   @CommandDefinition(name = "test", description = "Tests a data source", activator = ConnectionActivator.class)
   public static class Test extends RestCliCommand {

      @Argument(required = true, description = "The name of the data source", completer = DataSourceCompleter.class)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.server().dataSourceTest(name);
      }
   }
}
