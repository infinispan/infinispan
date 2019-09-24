package org.infinispan.cli.commands;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(CliCommand.class)
@CommandDefinition(name = "connect", description = "Connects to a remote server")
public class Connect extends CliCommand {
   @Argument(description = "The connection string")
   String connectionString;

   @Option(shortName = 'u')
   String username;

   @Option(shortName = 'p')
   String password;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      if (username != null) {
         invocation.getContext().connect(invocation.getShell(), connectionString, username, password);
      } else {
         invocation.getContext().connect(invocation.getShell(), connectionString);
      }
      return invocation.getContext().isConnected() ? CommandResult.SUCCESS : CommandResult.FAILURE;
   }
}
