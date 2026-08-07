package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 16.3
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "lpwd", description = "Prints the local working directory")
public class Lpwd extends CliCommand {

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      invocation.println(invocation.getContext().getCurrentWorkingDirectory().getAbsolutePath());
      return CommandResult.SUCCESS;
   }
}
