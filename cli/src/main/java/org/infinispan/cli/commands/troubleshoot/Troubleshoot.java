package org.infinispan.cli.commands.troubleshoot;

import org.aesh.command.Command;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.commands.troubleshoot.log.AccessLogParse;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * Group utilities commands to troubleshoot the system.
 *
 * <p>
 * Troubleshooting vary to parse access log files to identify long-running operations up to verifying network connectivity
 * between the cluster members.
 * </p>
 *
 * @author José Bolina
 * @since 15.0
 */
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "troubleshoot", description = "Execute troubleshooting commands", groupCommands = { AccessLogParse.class, PersistentStateParse.class })
public class Troubleshoot extends CliCommand {

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   protected boolean isHelp() {
      return help;
   }

   @Override
   protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      invocation.println(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }
}
