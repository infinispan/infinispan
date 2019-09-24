package org.infinispan.cli.commands;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.infinispan.cli.activators.DisabledActivator;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(CliCommand.class)
@CommandDefinition(name = "revoke", description = "Revokes access", aliases = "deny", activator = DisabledActivator.class)
public class Revoke extends CliCommand {

   @Override
   public CommandResult exec(ContextAwareCommandInvocation commandInvocation) {
      return CommandResult.SUCCESS;
   }
}
