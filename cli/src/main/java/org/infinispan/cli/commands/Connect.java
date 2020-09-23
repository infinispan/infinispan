package org.infinispan.cli.commands;

import org.aesh.command.Command;
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
@MetaInfServices(Command.class)
@CommandDefinition(name = Connect.CMD, description = "Connects to a remote server")
public class Connect extends CliCommand {
   public static final String CMD = "connect";
   @Argument(description = "The connection string 'http://<host>:<port>")
   String connectionString;

   @Option(shortName = 'u')
   String username;

   @Option(shortName = 'p')
   String password;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

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
