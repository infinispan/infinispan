package org.infinispan.cli.commands;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.infinispan.cli.activators.DisabledActivator;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(CliCommand.class)
@CommandDefinition(name = "evict", description = "Evicts an entry from the cache", activator = DisabledActivator.class)
public class Evict extends CliCommand {
   @Argument
   String key;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation commandInvocation) {
      return CommandResult.SUCCESS;
   }
}
