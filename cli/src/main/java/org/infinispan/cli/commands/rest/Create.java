package org.infinispan.cli.commands.rest;

import java.io.File;
import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.CacheConfigurationCompleter;
import org.infinispan.cli.completers.CounterStorageCompleter;
import org.infinispan.cli.completers.CounterTypeCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "create", description = "Creates a cache or a counter", activator = ConnectionActivator.class, groupCommands = {Create.Cache.class, Create.Counter.class})
public class Create extends CliCommand {

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

   @CommandDefinition(name = "cache", description = "Create a cache", activator = ConnectionActivator.class)
   public static class Cache extends RestCliCommand {

      @Argument(required = true)
      String name;

      @Option(completer = CacheConfigurationCompleter.class, shortName = 't')
      String template;

      @Option(completer = FileOptionCompleter.class, shortName = 'f')
      Resource file;

      @Option(defaultValue = "false", name = "volatile", shortName = 'v')
      boolean volatileCache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         if (template != null && file != null) {
            throw Messages.MSG.mutuallyExclusiveOptions("template", "file");
         }
         if (template == null && file == null) {
            throw Messages.MSG.requiresOneOf("template", "file");
         }

         RestCacheClient cache = client.cache(name);
         CacheContainerAdmin.AdminFlag flags[] = volatileCache ? new CacheContainerAdmin.AdminFlag[]{CacheContainerAdmin.AdminFlag.VOLATILE} : new CacheContainerAdmin.AdminFlag[]{};
         if (template != null) {
            return cache.createWithTemplate(template, flags);
         } else {
            return cache.createWithConfiguration(RestEntity.create(new File(file.getAbsolutePath())), flags);
         }
      }
   }

   @CommandDefinition(name = "counter", description = "Create a counter", activator = ConnectionActivator.class)
   public static class Counter extends RestCliCommand {

      @Argument(required = true)
      String name;

      @Option(shortName = 't', defaultValue = "", completer = CounterTypeCompleter.class, description = "Type of counter [weak|strong]")
      String type;

      @Option(shortName = 'i', name = "initial-value", defaultValue = "0", description = "Initial value for the counter (defaults to 0)")
      Long initialValue;

      @Option(shortName = 's', defaultValue = "VOLATILE", completer = CounterStorageCompleter.class, description = "persistent state PERSISTENT | VOLATILE (default)")
      String storage;

      @Option(shortName = 'u', name = "upper-bound")
      Long upperBound;

      @Option(shortName = 'l', name = "lower-bound")
      Long lowerBound;

      @Option(shortName = 'c', name = "concurrency-level", defaultValue = "16", description = "concurrency level for weak counters, defaults to 16")
      Integer concurrencyLevel;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         Json counterBody = Json.object()
               .set("initial-value", initialValue)
               .set("storage", storage)
               .set("name", name);
         if ("weak".equals(type)) {
            counterBody.set("concurrency-level", concurrencyLevel);
         }
         if (upperBound != null) {
            counterBody.set("upper-bound", upperBound);
         }
         if (lowerBound != null) {
            counterBody.set("lower-bound", lowerBound);
         }
         Json counter = Json.object().set(type + "-counter", counterBody);
         return client.counter(name).create(RestEntity.create(MediaType.APPLICATION_JSON, counter.toString()));
      }
   }
}
