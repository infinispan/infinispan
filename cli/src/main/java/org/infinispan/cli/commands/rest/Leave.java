package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.aesh.command.Command;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;
import org.openjdk.jmh.runner.options.TimeValue;

@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "leave", description = "Stop all caches in a cache container instance", activator = ConnectionActivator.class)
public class Leave extends RestCliCommand {

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Option(defaultValue = "", description = "The timeout to wait for caches to complete pending operations (e.g., 30s)")
   protected String timeout;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) throws Exception {
      if (timeout.isEmpty()) {
         return client.container().leave();
      }

      TimeValue value = TimeValue.fromString(timeout);
      return client.container().leave(value.convertTo(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
   }
}
