package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.impl.completer.BooleanOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CounterCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.CounterResource;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "cas", description = "Compares and sets counter values", activator = ConnectionActivator.class)
public class Cas extends RestCliCommand {

   @Argument(completer = CounterCompleter.class)
   String counter;

   @Option(description = "Does not display the value", completer = BooleanOptionCompleter.class)
   boolean quiet;

   @Option(required = true, description = "The expected value")
   long expect;

   @Option(required = true, description = "The new value")
   long value;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
      RestCounterClient cc = counter != null ? client.counter(counter) : client.counter(CounterResource.counterName(resource));
      if (quiet) {
         return cc.compareAndSet(expect, value);
      } else {
         return cc.compareAndSwap(expect, value);
      }
   }
}
