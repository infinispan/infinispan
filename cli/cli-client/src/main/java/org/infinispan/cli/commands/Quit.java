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
@CommandDefinition(name = Quit.CMD, description = "Quits the CLI")
public class Quit extends CliCommand {

   public static final String CMD = "quit";

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      invocation.getContext().disconnect();
      invocation.stop();
      return CommandResult.SUCCESS;
   }
}
