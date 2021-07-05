package org.infinispan.cli.commands.rest;

import static org.infinispan.cli.logging.Messages.MSG;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.CdContextCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.cli.resources.ContainerResource;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Ryan Emerson
 * @since 13.0
 */
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "rebalance", description = "Manage rebalance behaviour", activator = ConnectionActivator.class, groupCommands = {Rebalance.Enable.class, Rebalance.Disable.class})
public class Rebalance extends CliCommand {

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

   @CommandDefinition(name = "enable", description = "Enable rebalancing", activator = ConnectionActivator.class)
   public static class Enable extends RestCliCommand {
      @Argument(description = "The path of the resource", completer = CdContextCompleter.class)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource activeResource) {
         try {
            Resource resource = activeResource.getResource(name);
            if (resource instanceof CacheResource) {
               return client.cache(resource.getName()).enableRebalancing();
            } else if (resource instanceof ContainerResource) {
               return client.cacheManager(resource.getName()).enableRebalancing();
            } else {
               String name = resource.getName();
               throw MSG.invalidResource(name.isEmpty() ? "/" : name);
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
   }

   @CommandDefinition(name = "disable", description = "Disable rebalancing", activator = ConnectionActivator.class)
   public static class Disable extends RestCliCommand {
      @Argument(description = "The path of the resource", completer = CdContextCompleter.class)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource activeResource) {
         try {
            Resource resource = activeResource.getResource(name);
            if (resource instanceof CacheResource) {
               return client.cache(resource.getName()).disableRebalancing();
            } else if (resource instanceof ContainerResource) {
               return client.cacheManager(resource.getName()).disableRebalancing();
            } else {
               String name = resource.getName();
               throw MSG.invalidResource(name.isEmpty() ? "/" : name);
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
   }
}
