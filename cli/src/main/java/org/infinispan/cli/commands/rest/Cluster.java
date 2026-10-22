package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 16.4
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "cluster", description = "Obtains information about the cluster", activator = ConnectionActivator.class, groupCommands = {Cluster.Membership.class})
public class Cluster extends CliCommand {


   @Override
   public CommandResult exec(ContextAwareCommandInvocation commandInvocation) {
      commandInvocation.println(commandInvocation.getHelpInfo());
      return CommandResult.SUCCESS;
   }

   @CommandDefinition(name = "membership", description = "Obtains cluster membership information", activator = ConnectionActivator.class)
   public static class Membership extends RestCliCommand {

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cluster().clusterMembers();
      }
   }
}
