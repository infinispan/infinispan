package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.activators.DisabledActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.converters.NullableIntegerConverter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "migrate", description = "Migration operations", groupCommands = {Migrate.Cluster.class})
public class Migrate extends CliCommand {

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

   @GroupCommandDefinition(name = "cluster", description = "Performs data migration between clusters", groupCommands = {Migrate.ClusterConnect.class, Migrate.ClusterDisconnect.class, Migrate.ClusterSynchronize.class}, activator = ConnectionActivator.class)
   public static class Cluster extends CliCommand {

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
   }

   @CommandDefinition(name = "connect", description = "Connects to a source cluster", activator = DisabledActivator.class)
   public static class ClusterConnect extends CliCommand {

      @Option(completer = CacheCompleter.class, shortName = 'c', description = "The name of the cache.")
      String cache;

      @Option(shortName = 'u', description = "The Hot Rod URL for the source cluster", required = true)
      String url;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         // TODO: ISPN-11870
         throw new UnsupportedOperationException();
      }
   }

   @CommandDefinition(name = "disconnect", description = "Disconnects from a source cluster")
   public static class ClusterDisconnect extends RestCliCommand {

      @Option(completer = CacheCompleter.class, shortName = 'c', description = "The name of the cache.")
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache != null ? cache : CacheResource.cacheName(resource)).disconnectSource();
      }
   }

   @CommandDefinition(name = "synchronize", description = "Synchronizes data from a source to a target cluster")
   public static class ClusterSynchronize extends RestCliCommand {

      @Option(completer = CacheCompleter.class, shortName = 'c')
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Option(shortName = 'b', name = "read-batch", description = "The amount of entries to process in a batch", converter = NullableIntegerConverter.class)
      Integer readBatch;

      @Option(shortName = 't', description = "The number of threads to use. Defaults to the number of cores on the server", converter = NullableIntegerConverter.class)
      Integer threads;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache != null ? cache : CacheResource.cacheName(resource)).synchronizeData(readBatch, threads);
      }
   }
}
