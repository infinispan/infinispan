package org.infinispan.cli.commands;

import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.ServerCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = Shutdown.CMD, description = "Shuts down individual servers or the entire cluster", activator = ConnectionActivator.class, groupCommands = {Shutdown.Server.class, Shutdown.Cluster.class})
public class Shutdown extends CliCommand {
   public static final String CMD = "shutdown";
   public static final String SERVERS = "servers";

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      // This command serves only to wrap the sub-commands
      invocation.println(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   @CommandDefinition(name = Shutdown.Server.CMD, description = "Shuts down one or more individual servers", activator = ConnectionActivator.class)
   public static class Server extends CliCommand {
      public static final String CMD = "server";

      @Arguments(completer = ServerCompleter.class)
      List<String> servers;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         if (help) {
            invocation.println(invocation.getHelpInfo());
         }
         CommandInputLine cmd = new CommandInputLine(Shutdown.CMD)
               .arg(TYPE, Shutdown.Server.CMD)
               .arg(SERVERS, servers);
         return invocation.execute(cmd);
      }
   }

   @CommandDefinition(name = Shutdown.Cluster.CMD, description = "Shuts down the entire cluster", activator = ConnectionActivator.class)
   public static class Cluster extends CliCommand {
      public static final String CMD = "cluster";

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         if (help) {
            invocation.println(invocation.getHelpInfo());
         }
         CommandInputLine cmd = new CommandInputLine(Shutdown.CMD)
               .arg(TYPE, Shutdown.Cluster.CMD);
         return invocation.execute(cmd);
      }
   }
}
