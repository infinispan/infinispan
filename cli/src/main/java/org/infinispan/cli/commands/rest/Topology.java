package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "topology", description = "Manages the cluster topology.",
      activator = ConnectionActivator.class, groupCommands = {Topology.SetStable.class})
public class Topology extends CliCommand {

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   protected boolean isHelp() {
      return help;
   }

   @Override
   protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      invocation.println(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   @CommandDefinition(name = "set-stable", description = "Set the current topology as stable.", activator = ConnectionActivator.class)
   public static class SetStable extends RestCliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Option(shortName = 'f', hasValue = false, overrideRequired = true)
      protected boolean force;

      @Argument(description = "The cache name to mark topology stable.", completer = CacheCompleter.class)
      String cacheName;

      @Override
      protected boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) throws Exception {
         return client.cache(cacheName != null ? cacheName : CacheResource.cacheName(resource)).markTopologyStable(force);
      }
   }
}
