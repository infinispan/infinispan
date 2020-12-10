package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.CacheCompleter;
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
@GroupCommandDefinition(name = "site", description = "Manages backup sites",
      activator = ConnectionActivator.class,
      groupCommands = {
            Site.Status.class,
            Site.BringOnline.class,
            Site.TakeOffline.class,
            Site.PushSiteState.class,
            Site.CancelPushState.class,
            Site.CancelReceiveState.class,
            Site.PushSiteStatus.class,
            Site.ClearPushStateStatus.class,
      }
)
public class Site extends CliCommand {

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

   @CommandDefinition(name = "status", description = "Shows site status", activator = ConnectionActivator.class)
   public static class Status extends RestCliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return site == null ? client.cache(cache).xsiteBackups() : client.cache(cache).backupStatus(site);
      }
   }

   @CommandDefinition(name = "bring-online", description = "Brings a site online", activator = ConnectionActivator.class)
   public static class BringOnline extends RestCliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(required = true)
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).bringSiteOnline(site);
      }
   }

   @CommandDefinition(name = "take-offline", description = "Takes a site offline", activator = ConnectionActivator.class)
   public static class TakeOffline extends RestCliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(required = true)
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).takeSiteOffline(site);
      }
   }

   @CommandDefinition(name = "push-site-state", description = "Starts pushing state to a site", activator = ConnectionActivator.class)
   public static class PushSiteState extends RestCliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(required = true)
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).pushSiteState(site);
      }
   }

   @CommandDefinition(name = "cancel-push-state", description = "Cacncels pushing state to a site", activator = ConnectionActivator.class)
   public static class CancelPushState extends RestCliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(required = true)
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).cancelPushState(site);
      }
   }

   @CommandDefinition(name = "cancel-receive-state", description = "Cancels receiving state to a site", activator = ConnectionActivator.class)
   public static class CancelReceiveState extends RestCliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(required = true)
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).cancelReceiveState(site);
      }
   }

   @CommandDefinition(name = "push-site-status", description = "Shows the status of pushing to a site", activator = ConnectionActivator.class)
   public static class PushSiteStatus extends RestCliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).pushStateStatus();
      }
   }

   @CommandDefinition(name = "clear-push-site-status", description = "Clears the push state status", activator = ConnectionActivator.class)
   public static class ClearPushStateStatus extends RestCliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).clearPushStateStatus();
      }
   }
}
