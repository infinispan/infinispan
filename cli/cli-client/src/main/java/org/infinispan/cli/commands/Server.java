package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = Server.CMD, description = "Obtains information about the server", activator = ConnectionActivator.class, groupCommands = {Server.Report.class})
public class Server extends CliCommand {

   public static final String CMD = "server";
   public static final String NAME = "name";

   @Override
   public CommandResult exec(ContextAwareCommandInvocation commandInvocation) {
      commandInvocation.println(commandInvocation.getHelpInfo());
      return CommandResult.SUCCESS;
   }

   @CommandDefinition(name = Report.CMD, description = "Obtains an aggregate report from the server", activator = ConnectionActivator.class)
   public static class Report extends CliCommand {
      public static final String CMD = "report";

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(Server.CMD)
               .arg(TYPE, Report.CMD);
         return invocation.execute(cmd);
      }
   }
}
