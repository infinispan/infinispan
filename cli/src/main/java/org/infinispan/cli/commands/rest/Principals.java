package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;

/**
 * @since 15.0
 **/
@GroupCommandDefinition(name = Principals.CMD, description = "Performs operations on principals", activator = ConnectionActivator.class, groupCommands = {Principals.Ls.class})
public class Principals extends CliCommand {

   public static final String CMD = "principals";


   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      // This command serves only to wrap the sub-commands
      invocation.println(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   @CommandDefinition(name = "ls", description = "Lists principals", activator = ConnectionActivator.class)
   public static class Ls extends RestCliCommand {

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.security().listPrincipals();
      }
   }
}
