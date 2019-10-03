package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.infinispan.cli.activators.DisabledActivator;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Abort.CMD, description = "Aborts a batch", activator = DisabledActivator.class)
public class Abort extends CliCommand {

   public static final String CMD = "abort";

   @Override
   public CommandResult exec(ContextAwareCommandInvocation commandInvocation) {
      return CommandResult.SUCCESS;
   }

   @Override
   public int nesting() {
      return -1;
   }
}
