package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.impl.completer.BooleanOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CounterCompleter;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.CounterResource;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "add", description = "Adds/subtracts a value to/from a counter", activator = ConnectionActivator.class)
public class Add extends RestCliCommand {

   @Argument(completer = CounterCompleter.class, description = "The name of the counter")
   String counter;

   @Option(description = "Does not display the value", completer = BooleanOptionCompleter.class)
   boolean quiet;

   @Option(description = "The delta to add/subtract from/to the value. Defaults to adding 1", defaultValue = "1")
   long delta;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
      return client.counter(counter != null ? counter : CounterResource.counterName(resource)).add(delta);
   }

   @Override
   public Connection.ResponseMode getResponseMode() {
      return quiet ? Connection.ResponseMode.QUIET : Connection.ResponseMode.BODY;
   }
}
