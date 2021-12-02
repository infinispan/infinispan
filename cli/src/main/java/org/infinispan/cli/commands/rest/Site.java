package org.infinispan.cli.commands.rest;

import static org.infinispan.cli.logging.Messages.MSG;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Option;
import org.aesh.command.parser.RequiredOptionException;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CacheAwareCommand;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.completers.SiteCompleter;
import org.infinispan.cli.completers.XSiteStateTransferModeCompleter;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.cli.resources.ContainerResource;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
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
            Site.View.class,
            Site.Name.class,
            Site.StateTransferMode.class,
            Site.IsRelayNode.class,
            Site.RelayNodes.class
      }
)
public class Site extends CliCommand {

   private static final Supplier<RequiredOptionException> MISSING_CACHE_OR_GLOBAL = () -> MSG.requiresOneOf("cache", "all-caches");

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
   public static class Status extends RestCliCommand implements CacheAwareCommand {
      @Option(shortName = 'c', completer = CacheCompleter.class, description = "The cache name.")
      String cache;

      @Option(shortName = 'a', name = "all-caches", hasValue = false, description = "Invoke operation in all caches.")
      boolean allCaches;

      @Option(shortName = 's', completer = SiteCompleter.class, description = "The remote backup name.")
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) throws RequiredOptionException {
         checkMutualExclusiveCacheAndGlobal(cache, allCaches);
         if (allCaches) {
            RestCacheManagerClient cm = restCacheManagerClient(client, resource);
            return site == null ? cm.backupStatuses() : cm.backupStatus(site);
         }
         RestCacheClient c = restCacheClient(client, resource, cache);
         return site == null ? c.xsiteBackups() : c.backupStatus(site);
      }

      @Override
      public Optional<String> getCacheName(Resource activeResource) {
         return cache == null ? CacheResource.findCacheName(activeResource) : Optional.of(cache);
      }
   }

   @CommandDefinition(name = "bring-online", description = "Brings a site online", activator = ConnectionActivator.class)
   public static class BringOnline extends RestCliCommand implements CacheAwareCommand {
      @Option(shortName = 'c', completer = CacheCompleter.class, description = "The cache name.")
      String cache;

      @Option(shortName = 'a', name = "all-caches", hasValue = false, description = "Invoke operation in all caches.")
      boolean allCaches;

      @Option(required = true, shortName = 's', completer = SiteCompleter.class, description = "The remote backup name.")
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) throws RequiredOptionException {
         checkMutualExclusiveCacheAndGlobal(cache, allCaches);
         return allCaches ?
               restCacheManagerClient(client, resource).bringBackupOnline(site) :
               restCacheClient(client, resource, cache).bringSiteOnline(site);
      }

      @Override
      public Optional<String> getCacheName(Resource activeResource) {
         return cache == null ? CacheResource.findCacheName(activeResource) : Optional.of(cache);
      }
   }

   @CommandDefinition(name = "take-offline", description = "Takes a site offline", activator = ConnectionActivator.class)
   public static class TakeOffline extends RestCliCommand implements CacheAwareCommand {
      @Option(shortName = 'c', completer = CacheCompleter.class, description = "The cache name.")
      String cache;

      @Option(shortName = 'a', name = "all-caches", hasValue = false, description = "Invoke operation in all caches.")
      boolean allCaches;

      @Option(required = true, shortName = 's', completer = SiteCompleter.class, description = "The remote backup name.")
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) throws RequiredOptionException {
         checkMutualExclusiveCacheAndGlobal(cache, allCaches);
         return allCaches ?
               restCacheManagerClient(client, resource).takeOffline(site) :
               restCacheClient(client, resource, cache).takeSiteOffline(site);
      }

      @Override
      public Optional<String> getCacheName(Resource activeResource) {
         return cache == null ? CacheResource.findCacheName(activeResource) : Optional.of(cache);
      }
   }

   @CommandDefinition(name = "push-site-state", description = "Starts pushing state to a site", activator = ConnectionActivator.class)
   public static class PushSiteState extends RestCliCommand implements CacheAwareCommand {
      @Option(shortName = 'c', completer = CacheCompleter.class, description = "The cache name.")
      String cache;

      @Option(shortName = 'a', name = "all-caches", hasValue = false, description = "Invoke operation in all caches.")
      boolean allCaches;

      @Option(required = true, shortName = 's', completer = SiteCompleter.class, description = "The remote backup name.")
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) throws RequiredOptionException {
         checkMutualExclusiveCacheAndGlobal(cache, allCaches);
         return allCaches ?
               restCacheManagerClient(client, resource).pushSiteState(site) :
               restCacheClient(client, resource, cache).pushSiteState(site);
      }

      @Override
      public Optional<String> getCacheName(Resource activeResource) {
         return cache == null ? CacheResource.findCacheName(activeResource) : Optional.of(cache);
      }
   }

   @CommandDefinition(name = "cancel-push-state", description = "Cancels pushing state to a site", activator = ConnectionActivator.class)
   public static class CancelPushState extends RestCliCommand implements CacheAwareCommand {
      @Option(shortName = 'c', completer = CacheCompleter.class, description = "The cache name.")
      String cache;

      @Option(shortName = 'a', name = "all-caches", hasValue = false, description = "Invoke operation in all caches.")
      boolean allCaches;

      @Option(required = true, shortName = 's', completer = SiteCompleter.class, description = "The remote backup name.")
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) throws RequiredOptionException {
         checkMutualExclusiveCacheAndGlobal(cache, allCaches);
         return allCaches ?
               restCacheManagerClient(client, resource).cancelPushState(site) :
               restCacheClient(client, resource, cache).cancelPushState(site);
      }

      @Override
      public Optional<String> getCacheName(Resource activeResource) {
         return cache == null ? CacheResource.findCacheName(activeResource) : Optional.of(cache);
      }
   }

   @CommandDefinition(name = "cancel-receive-state", description = "Cancels receiving state to a site", activator = ConnectionActivator.class)
   public static class CancelReceiveState extends RestCliCommand implements CacheAwareCommand {
      @Option(required = true, shortName = 'c', completer = CacheCompleter.class, description = "The cache name.")
      String cache;

      @Option(required = true, shortName = 's', completer = SiteCompleter.class, description = "The remote backup name.")
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

      @Override
      public Optional<String> getCacheName(Resource activeResource) {
         return Optional.ofNullable(cache);
      }
   }

   @CommandDefinition(name = "push-site-status", description = "Shows the status of pushing to a site", activator = ConnectionActivator.class)
   public static class PushSiteStatus extends RestCliCommand implements CacheAwareCommand {
      @Option(required = true, shortName = 'c', completer = CacheCompleter.class, description = "The cache name.")
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

      @Override
      public Optional<String> getCacheName(Resource activeResource) {
         return Optional.ofNullable(cache);
      }
   }

   @CommandDefinition(name = "clear-push-site-status", description = "Clears the push state status", activator = ConnectionActivator.class)
   public static class ClearPushStateStatus extends RestCliCommand implements CacheAwareCommand {
      @Option(required = true, shortName = 'c', completer = CacheCompleter.class, description = "The cache name.")
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

      @Override
      public Optional<String> getCacheName(Resource activeResource) {
         return Optional.ofNullable(cache);
      }
   }

   @CommandDefinition(name = "view", description = "Prints the global sites view", activator = ConnectionActivator.class)
   public static class View extends CliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         try {
            Connection connection = invocation.getContext().getConnection();
            connection.refreshServerInfo();
            invocation.println(connection.getSitesView().toString());
            return CommandResult.SUCCESS;
         } catch (IOException e) {
            throw new CommandException(e);
         }
      }

   }

   @CommandDefinition(name = "name", description = "Prints the local site name", activator = ConnectionActivator.class)
   public static class Name extends CliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         try {
            Connection connection = invocation.getContext().getConnection();
            connection.refreshServerInfo();
            invocation.println(connection.getLocalSiteName());
            return CommandResult.SUCCESS;
         } catch (IOException e) {
            throw new CommandException(e);
         }
      }
   }

   @GroupCommandDefinition(name = "state-transfer-mode", description = "Controls the cross-site state transfer mode.",
         activator = ConnectionActivator.class,
         groupCommands = {
               GetStateTransferMode.class,
               SetStateTransferMode.class
         })
   public static class StateTransferMode extends CliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         // This command serves only to wrap the sub-commands
         invocation.println(invocation.getHelpInfo());
         return CommandResult.FAILURE;
      }
   }

   @CommandDefinition(name = "get", description = "Retrieves the cross-site state transfer mode.", activator = ConnectionActivator.class)
   public static class GetStateTransferMode extends RestCliCommand implements CacheAwareCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Option(shortName = 'c', completer = CacheCompleter.class, description = "The cache name.")
      String cache;

      @Option(shortName = 's', required = true, completer = SiteCompleter.class, description = "The remote backup name.")
      String site;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public Optional<String> getCacheName(Resource activeResource) {
         return cache == null ? CacheResource.findCacheName(activeResource) : Optional.of(cache);
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         String cacheName = getCacheName(resource).orElseThrow(MSG::illegalContext);
         return client.cache(cacheName).xSiteStateTransferMode(site);
      }
   }

   @CommandDefinition(name = "set", description = "Sets the cross-site state transfer mode.", activator = ConnectionActivator.class)
   public static class SetStateTransferMode extends RestCliCommand implements CacheAwareCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Option(shortName = 'c', completer = CacheCompleter.class, description = "The cache name.")
      String cache;

      @Option(shortName = 's', required = true, completer = SiteCompleter.class, description = "The remote backup name.")
      String site;

      @Option(shortName = 'm', required = true, completer = XSiteStateTransferModeCompleter.class, description = "The state transfer mode to set.")
      protected String mode;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         String cacheName = getCacheName(resource).orElseThrow(MSG::illegalContext);
         return client.cache(cacheName).xSiteStateTransferMode(site, XSiteStateTransferMode.valueOf(mode.toUpperCase()));
      }

      @Override
      public Optional<String> getCacheName(Resource activeResource) {
         return cache == null ? CacheResource.findCacheName(activeResource) : Optional.of(cache);
      }
   }

   @CommandDefinition(name = "relay-nodes", description = "Returns the list of relay nodes.", activator = ConnectionActivator.class)
   public static class RelayNodes extends CliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         try {
            Connection connection = invocation.getContext().getConnection();
            connection.refreshServerInfo();
            invocation.println(String.valueOf(connection.getRelayNodes()));
            return CommandResult.SUCCESS;
         } catch (IOException e) {
            throw new CommandException(e);
         }
      }
   }

   @CommandDefinition(name = "is-relay-node", description = "Returns true if the node handles relay messages between clusters.", activator = ConnectionActivator.class)
   public static class IsRelayNode extends CliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         try {
            Connection connection = invocation.getContext().getConnection();
            connection.refreshServerInfo();
            invocation.println(String.valueOf(connection.isRelayNode()));
            return CommandResult.SUCCESS;
         } catch (IOException e) {
            throw new CommandException(e);
         }
      }
   }

   private static void checkMutualExclusiveCacheAndGlobal(String cache, boolean global) {
      if (cache != null && global) {
         throw MSG.mutuallyExclusiveOptions("cache", "all-caches");
      }
   }

   private static RestCacheManagerClient restCacheManagerClient(RestClient client, Resource resource) {
      return ContainerResource.findContainerName(resource).map(client::cacheManager).orElseThrow(MSG::illegalContext);
   }

   private static RestCacheClient restCacheClient(RestClient client, Resource resource, String cacheName) throws RequiredOptionException {
      return cacheName == null ?
            CacheResource.findCacheName(resource).map(client::cache).orElseThrow(MISSING_CACHE_OR_GLOBAL) :
            client.cache(cacheName);
   }
}
