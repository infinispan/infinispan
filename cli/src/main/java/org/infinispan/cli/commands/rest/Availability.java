package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.AvailabilityCompleter;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * Manage a cache's Availability.
 *
 * @author Ryan Emerson
 * @since 13.0
 */
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "availability", description = "Manage availability of clustered caches in network partitions.", activator = ConnectionActivator.class)
public class Availability extends RestCliCommand {

   @Argument(completer = CacheCompleter.class)
   String cache;

   @Option(shortName = 'm', completer = AvailabilityCompleter.class)
   String mode;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
      RestCacheClient cacheClient = client.cache(cache != null ? cache : CacheResource.cacheName(resource));
      return mode == null ?
            cacheClient.getAvailability() :
            cacheClient.setAvailability(AvailabilityCompleter.Availability.valueOf(mode).toString());
   }
}
