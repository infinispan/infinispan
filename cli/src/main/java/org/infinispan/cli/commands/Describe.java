package org.infinispan.cli.commands;

import java.io.IOException;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CdContextCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "describe", description = "Displays information about the specified resource", activator = ConnectionActivator.class)
public class Describe extends CliCommand {

   @Argument(description = "The path of the resource", completer = CdContextCompleter.class)
   String name;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      try {
         invocation.println(invocation.getContext().getConnection().getActiveResource().getResource(name).describe());
         return CommandResult.SUCCESS;
      } catch (IOException e) {
         throw new CommandException(e);
      }
   }
}
