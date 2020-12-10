package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.ContainerCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.ContainersResource;
import org.infinispan.cli.resources.RootResource;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "container", description = "Selects a container", activator = ConnectionActivator.class)
public class Container extends CliCommand {

   @Argument(description = "The name of the container", completer = ContainerCompleter.class, required = true)
   String name;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      return invocation.getContext().changeResource(RootResource.class, ContainersResource.NAME, name);
   }
}
