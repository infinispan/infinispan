package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
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
 * @since 14.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "index", description = "Performs operations on indexes", activator = ConnectionActivator.class, groupCommands = {Index.Reindex.class, Index.Clear.class, Index.Stats.class, Index.UpdateIndex.class, Index.ClearStats.class})
public class Index extends CliCommand {

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation commandInvocation) {
      commandInvocation.println(commandInvocation.getHelpInfo());
      return CommandResult.SUCCESS;
   }

   @CommandDefinition(name = "reindex", description = "Reindexes a cache.", activator = ConnectionActivator.class)
   public static class Reindex extends RestCliCommand {

      @Argument(description = "Specifies which cache to reindex.", completer = CacheCompleter.class, required = true)
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).reindex();
      }
   }

   @CommandDefinition(name = "clear", description = "Clears a cache index.", activator = ConnectionActivator.class)
   public static class Clear extends RestCliCommand {

      @Argument(description = "Specifies which cache index to clear.", completer = CacheCompleter.class, required = true)
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).clearIndex();
      }
   }

   @CommandDefinition(name = "update-schema", description = "Update index schema for a given cache.", activator = ConnectionActivator.class)
   public static class UpdateIndex extends RestCliCommand {

      @Argument(description = "Specifies which cache to update its index schema.", completer = CacheCompleter.class, required = true)
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).updateIndexSchema();
      }
   }

   @CommandDefinition(name = "stats", description = "Displays indexing and search statistics for a cache.", activator = ConnectionActivator.class)
   public static class Stats extends RestCliCommand {

      @Argument(description = "Specifies which cache statistics to display.", completer = CacheCompleter.class, required = true)
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).searchStats();
      }
   }

   @CommandDefinition(name = "clear-stats", description = "Clears cache indexing and search statistics.", activator = ConnectionActivator.class)
   public static class ClearStats extends RestCliCommand {

      @Argument(description = "Specifies which cache statistics to clear.", completer = CacheCompleter.class, required = true)
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.cache(cache).clearSearchStats();
      }
   }
}
