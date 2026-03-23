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
@GroupCommandDefinition(name = AclCache.CMD, description = "Performs operations on principals", activator = ConnectionActivator.class, groupCommands = {AclCache.Flush.class})
public class AclCache extends CliCommand {

   public static final String CMD = "aclcache";


   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      // This command serves only to wrap the sub-commands
      invocation.println(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   @CommandDefinition(name = "flush", description = "Flushes the ACL cache across the entire cluster", activator = ConnectionActivator.class)
   public static class Flush extends RestCliCommand {

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.security().flushCache();
      }
   }
}
