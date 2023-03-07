package org.infinispan.cli.commands.rest;

import static org.infinispan.cli.logging.Messages.MSG;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CdContextCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.cli.resources.ContainerResource;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "stats", description = "Shows cache and container statistics", activator = ConnectionActivator.class)
public class Stats extends RestCliCommand {

   @Argument(description = "The path of the resource", completer = CdContextCompleter.class)
   String name;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Option(hasValue = false)
   protected boolean reset;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource activeResource) {
      try {
         Resource resource = activeResource.getResource(name);
         if (resource instanceof CacheResource) {
            return reset ? client.cache(resource.getName()).statsReset() : client.cache(resource.getName()).stats();
         } else if (resource instanceof ContainerResource) {
            return reset ? client.cacheManager(resource.getName()).statsReset() : client.cacheManager(resource.getName()).stats();
         } else {
            if (reset) {
               throw MSG.cannotResetIndividualStat();
            }
            String name = resource.getName();
            throw MSG.invalidResource(name.isEmpty() ? "/" : name);
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }
}
