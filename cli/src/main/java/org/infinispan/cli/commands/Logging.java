package org.infinispan.cli.commands;

import java.util.Collections;
import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionList;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.LogAppenderCompleter;
import org.infinispan.cli.completers.LogLevelCompleter;
import org.infinispan.cli.completers.LoggersCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = Logging.CMD, description = "Inspects/Manipulates the server logging configuration", activator = ConnectionActivator.class, groupCommands = {Logging.Loggers.class, Logging.Appenders.class, Logging.Set.class, Logging.Remove.class})
public class Logging extends CliCommand {

   public static final String CMD = "logging";
   public static final String TYPE = "type";
   public static final String NAME = "name";

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

   @CommandDefinition(name = Loggers.CMD, description = "Lists available loggers", activator = ConnectionActivator.class)
   public static class Loggers extends CliCommand {
      public static final String CMD = "list-loggers";

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(Logging.CMD)
               .arg(TYPE, Loggers.CMD);
         return invocation.execute(cmd);
      }
   }

   @CommandDefinition(name = Appenders.CMD, description = "Lists available appenders", activator = ConnectionActivator.class)
   public static class Appenders extends CliCommand {
      public static final String CMD = "list-appenders";

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(Logging.CMD)
               .arg(TYPE, Appenders.CMD);
         return invocation.execute(cmd);
      }
   }

   @CommandDefinition(name = Remove.CMD, description = "Removes a logger", activator = ConnectionActivator.class)
   public static class Remove extends CliCommand {
      public static final String CMD = "remove";

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Argument(required = true, completer = LoggersCompleter.class)
      String name;

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(Logging.CMD)
               .arg(Logging.TYPE, Remove.CMD)
               .arg(Logging.NAME, name);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }

   @CommandDefinition(name = Set.CMD, description = "Sets a logger", activator = ConnectionActivator.class)
   public static class Set extends CliCommand {
      public static final String CMD = "set";
      public static final String LEVEL = "level";
      public static final String APPENDERS = "appenders";

      @Argument(completer = LoggersCompleter.class)
      String name;

      @Option(shortName = 'l', description = "One of OFF, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, ALL", completer = LogLevelCompleter.class)
      String level;

      @OptionList(shortName = 'a', description = "One or more appender names", completer = LogAppenderCompleter.class)
      List<String> appenders;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(Logging.CMD)
               .arg(Logging.TYPE, Set.CMD)
               .arg(Logging.NAME, name)
               .option(LEVEL, level)
               .optionalArg(APPENDERS, appenders);

         return invocation.execute(Collections.singletonList(cmd));
      }
   }
}
