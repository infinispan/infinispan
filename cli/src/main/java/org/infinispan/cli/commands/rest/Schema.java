package org.infinispan.cli.commands.rest;

import java.io.File;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.SchemaCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "schema", description = "Manipulates Protobuf schemas", activator = ConnectionActivator.class, groupCommands = {Schema.Upload.class, Schema.Remove.class, Schema.Ls.class, Schema.Get.class})
public class Schema extends CliCommand {
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

   @CommandDefinition(name = "upload", description = "Uploads Protobuf schemas to the server.")
   public static class Upload extends RestCliCommand {
      @Argument(required = true, description = "The name of the schema")
      String name;

      @Option(completer = FileOptionCompleter.class, shortName = 'f', description = "The Protobuf schema file to upload.", required = true)
      Resource file;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) throws NoSuchFileException {
         if (file.exists()) {
            return client.schemas().put(name, RestEntity.create(MediaType.TEXT_PLAIN, new File(file.getAbsolutePath())));
         } else {
            throw Messages.MSG.nonExistentFile(file);
         }
      }
   }

   @CommandDefinition(name = "ls", description = "Lists available Protobuf schemas.")
   public static class Ls extends RestCliCommand {
      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         return client.schemas().names();
      }
   }

   @CommandDefinition(name = "remove", aliases = "rm", description = "Deletes Protobuf schema.")
   public static class Remove extends RestCliCommand {
      @Argument(required = true, description = "The name of the schema", completer = SchemaCompleter.class)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         return client.schemas().delete(name);
      }
   }

   @CommandDefinition(name = "get", description = "Displays the Protobuf definition of a schema.")
   public static class Get extends RestCliCommand {
      @Argument(required = true, description = "The name of the schema", completer = SchemaCompleter.class)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         return client.schemas().get(name);
      }
   }
}
