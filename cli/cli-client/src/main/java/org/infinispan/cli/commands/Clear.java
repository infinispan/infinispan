package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Clear.CMD, description = "Clears the screen")
public class Clear extends CliCommand {

   public static final String CMD = "clear";

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      invocation.getShell().clear();
      return CommandResult.SUCCESS;
   }
}
