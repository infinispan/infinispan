package org.infinispan.cli.commands.rest;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;

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
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.TaskCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "task", description = "Executes or manipulates server-side tasks", activator = ConnectionActivator.class, groupCommands = {Task.Exec.class, Task.Upload.class})
public class Task extends CliCommand {
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

   @CommandDefinition(name = "exec", description = "Executes a server-side task", activator = ConnectionActivator.class)
   public static class Exec extends RestCliCommand {
      @Argument(completer = TaskCompleter.class, required = true)
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
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         return client.tasks().exec(taskName, parameters == null ? Collections.emptyMap() : parameters);
      }
   }

   @CommandDefinition(name = "upload", description = "Uploads a new script task to the server", activator = ConnectionActivator.class)
   public static class Upload extends RestCliCommand {
      @Argument(description = "The task name", required = true)
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
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         return client.tasks().uploadScript(taskName, RestEntity.create(MediaType.TEXT_PLAIN, new File(file.getAbsolutePath())));
      }
   }
}
