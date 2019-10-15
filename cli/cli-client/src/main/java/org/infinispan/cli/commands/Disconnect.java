package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Disconnect.CMD, description = "Disconnects from a remote server", activator = ConnectionActivator.class)
public class Disconnect extends CliCommand {

   public static final String CMD = "disconnect";

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      if (invocation.getContext().isConnected()) {
         invocation.getContext().disconnect();
         return CommandResult.SUCCESS;
      } else {
         invocation.getShell().writeln("Not connected");
         return CommandResult.FAILURE;
      }
   }
}
