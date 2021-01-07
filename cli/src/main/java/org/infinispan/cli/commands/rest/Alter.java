package org.infinispan.cli.commands.rest;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.command.parser.RequiredOptionException;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CacheAwareCommand;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.completers.CacheConfigurationAttributeCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "alter", description = "Alters a configuration", activator = ConnectionActivator.class, groupCommands = {Alter.Cache.class})
public class Alter extends CliCommand {

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

   @CommandDefinition(name = "cache", description = "Alters a cache configuration", activator = ConnectionActivator.class)
   public static class Cache extends RestCliCommand implements CacheAwareCommand {

      @Argument(completer = CacheCompleter.class, description = "The cache name")
      String name;

      @Option(description = "The configuration attribute", completer = CacheConfigurationAttributeCompleter.class)
      String attribute;

      @Option(description = "The value for the configuration attribute")
      String value;

      @Option(completer = FileOptionCompleter.class, shortName = 'f')
      Resource file;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) throws RequiredOptionException {
         if (attribute != null && file != null) {
            throw Messages.MSG.mutuallyExclusiveOptions("attribute", "file");
         }
         if (attribute == null && file == null) {
            throw Messages.MSG.requiresOneOf("attribute", "file");
         }
         if (attribute != null && value == null) {
            throw Messages.MSG.requiresAllOf("attribute", "value");
         }
         String n = getCacheName(resource).orElseThrow(() -> Messages.MSG.missingCacheName());
         if (file != null) {
            return client.cache(n).updateWithConfiguration(RestEntity.create(new File(file.getAbsolutePath())));
         } else {
            return client.cache(n).updateConfigurationAttribute(attribute, value);
         }
      }

      @Override
      public Optional<String> getCacheName(org.infinispan.cli.resources.Resource activeResource) {
         return name == null ? CacheResource.findCacheName(activeResource) : Optional.of(name);
      }
   }
}
