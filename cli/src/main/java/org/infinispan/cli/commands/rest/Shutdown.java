package org.infinispan.cli.commands.rest;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.ServerCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "shutdown", description = "Stops server instances and clusters.", activator = ConnectionActivator.class, groupCommands = {Shutdown.Server.class, Shutdown.Cluster.class, Shutdown.Container.class})
public class Shutdown extends CliCommand {

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

   @CommandDefinition(name = "server", description = "Stops one or more server instances.", activator = ConnectionActivator.class)
   public static class Server extends RestCliCommand {

      @Arguments(description = "Specifies server instances to stop.", completer = ServerCompleter.class)
      List<String> servers;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return servers == null || servers.isEmpty() ? client.server().stop() : client.cluster().stop(servers);
      }
   }

   @CommandDefinition(name = "cluster", description = "Stops all nodes in the cluster after storing cluster state and persisting entries if there is a cache store.", activator = ConnectionActivator.class)
   public static class Cluster extends RestCliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cluster().stop();
      }
   }

   @CommandDefinition(name = "container", description = "Stops the data container without terminating the server process.", activator = ConnectionActivator.class)
   public static class Container extends RestCliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.container().shutdown();
      }
   }
}
