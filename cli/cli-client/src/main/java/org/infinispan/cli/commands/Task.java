package org.infinispan.cli.commands;

import java.util.Collections;
import java.util.Map;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionGroup;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.TaskCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = Task.CMD, description = "Executes or manipulates server-side tasks", activator = ConnectionActivator.class, groupCommands = {Task.Exec.class, Task.Upload.class})
public class Task extends CliCommand {
   public static final String CMD = "task";

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

   @CommandDefinition(name = Task.Exec.CMD, description = "Executes a server-side task", activator = ConnectionActivator.class)
   public static class Exec extends CliCommand {
      public static final String CMD = "exec";
      public static final String PARAMETERS = "parameters";

      @Argument(completer = TaskCompleter.class)
      String taskName;

      @OptionGroup(shortName = 'P', description = "Task parameters")
      Map<String, String> parameters;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(Task.CMD)
               .arg(TYPE, Task.Exec.CMD)
               .arg(NAME, taskName)
               .arg(PARAMETERS, parameters == null ? Collections.emptyMap() : parameters);
         return invocation.execute(cmd);
      }
   }

   @CommandDefinition(name = Task.Upload.CMD, description = "Uploads a new script task to the server", activator = ConnectionActivator.class)
   public static class Upload extends CliCommand {
      public static final String CMD = "upload";

      @Argument(description = "The task name")
      String taskName;

      @Option(completer = FileOptionCompleter.class, shortName = 'f', required = true)
      Resource file;

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
         CommandInputLine cmd = new CommandInputLine(Task.CMD)
               .arg(TYPE, Task.Upload.CMD)
               .arg(NAME, taskName)
               .option(FILE, file.getAbsolutePath());
         return invocation.execute(cmd);
      }
   }
}
