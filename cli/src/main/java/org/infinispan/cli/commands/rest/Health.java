package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.option.Argument;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 16.3
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "health", description = "Shows cluster health or the health of an individual cache", activator = ConnectionActivator.class)
public class Health extends RestCliCommand {

   @Argument(description = "The name of a cache. If omitted, shows the cluster health.", completer = CacheCompleter.class)
   String cacheName;

   @Override
   protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
      if (cacheName != null && !cacheName.isEmpty()) {
         return client.cache(cacheName).health();
      } else {
         return client.container().healthStatus();
      }
   }
}
