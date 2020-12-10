package org.infinispan.cli.commands;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.infinispan.cli.patching.PatchTool;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "patch", description = "Patch operations", groupCommands = {Patch.Create.class, Patch.Describe.class, Patch.Install.class, Patch.Ls.class, Patch.Rollback.class})
public class Patch extends CliCommand {

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

   @CommandDefinition(name = "create", description = "Creates a patch archive")
   public static class Create extends CliCommand {

      @Option(defaultValue = "", shortName = 'q', description = "A qualifier for this patch (e.g. `one-off`)")
      String qualifier;

      @Arguments(completer = FileOptionCompleter.class, description = "The path to the patch archive, the path to the target server and one or more paths to the source servers")
      List<Resource> paths;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         if (paths == null || paths.size() < 3) {
            throw Messages.MSG.patchCreateArgumentsRequired();
         }
         PatchTool patchTool = new PatchTool(invocation.getShellOutput(), invocation.getShellError());
         try {
            Path patch = Paths.get(paths.get(0).getAbsolutePath());
            Path target = Paths.get(paths.get(1).getAbsolutePath());
            Path sources[] = new Path[paths.size() - 2];
            for (int i = 2; i < paths.size(); i++) {
               sources[i - 2] = Paths.get(paths.get(i).getAbsolutePath());
            }
            patchTool.createPatch(qualifier, patch, target, sources);
            return CommandResult.SUCCESS;
         } catch (IOException e) {
            throw new CommandException(e);
         }
      }
   }

   @CommandDefinition(name = "describe", description = "Describes the contents of a patch archive")
   public static class Describe extends CliCommand {

      @Argument(completer = FileOptionCompleter.class, description = "The path to a patch archive")
      Resource patch;

      @Option(shortName = 'v', hasValue = false, description = "List the contents of the patch including all the actions that will be performed")
      boolean verbose;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         if (patch == null) {
            throw Messages.MSG.patchArchiveArgumentRequired();
         }
         PatchTool patchTool = new PatchTool(invocation.getShellOutput(), invocation.getShellError());

         try {
            patchTool.describePatch(Paths.get(patch.getAbsolutePath()), verbose);
            return CommandResult.SUCCESS;
         } catch (IOException e) {
            throw new CommandException(e);
         }
      }
   }

   @CommandDefinition(name = "install", description = "Installs a patch archive")
   public static class Install extends CliCommand {

      @Argument(completer = FileOptionCompleter.class, description = "The path to a patch archive")
      Resource patch;

      @Option(completer = FileOptionCompleter.class, description = "The path to the server on which the patch will be installed.")
      Resource server;

      @Option(hasValue = false, name = "dry-run", description = "Only list the actions that will be performed without executing them.")
      boolean dryRun;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         if (patch == null) {
            throw Messages.MSG.patchArchiveArgumentRequired();
         }
         PatchTool patchTool = new PatchTool(invocation.getShellOutput(), invocation.getShellError());
         try {
            patchTool.installPatch(Paths.get(patch.getAbsolutePath()), getServerHome(server), dryRun);
            return CommandResult.SUCCESS;
         } catch (IOException e) {
            throw new CommandException(e);
         }
      }
   }

   @CommandDefinition(name = "ls", description = "Lists the patches installed on this server", aliases = "list")
   public static class Ls extends CliCommand {

      @Option(completer = FileOptionCompleter.class, description = "The path to the server installation.")
      Resource server;

      @Option(shortName = 'v', hasValue = false, description = "List the contents of all installed patches.")
      boolean verbose;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         PatchTool patchTool = new PatchTool(invocation.getShellOutput(), invocation.getShellError());
         patchTool.listPatches(getServerHome(server), verbose);
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "rollback", description = "Rolls back the latest patch installed on this server.")
   public static class Rollback extends CliCommand {

      @Option(completer = FileOptionCompleter.class, description = "The path to the server installation.")
      Resource server;

      @Option(hasValue = false, name = "dry-run", description = "Only list the actions that will be performed without executing them.")
      boolean dryRun;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         PatchTool patchTool = new PatchTool(invocation.getShellOutput(), invocation.getShellError());
         try {
            patchTool.rollbackPatch(getServerHome(server), dryRun);
            return CommandResult.SUCCESS;
         } catch (IOException e) {
            throw new CommandException(e);
         }
      }
   }

   public static Path getServerHome(Resource server) {
      if (server != null) {
         return Paths.get(server.getAbsolutePath());
      } else {
         String serverHome = System.getProperty("infinispan.server.home.path");
         if (serverHome != null) {
            return Paths.get(serverHome);
         } else {
            // Fall back to the cwd
            return Paths.get(System.getProperty("user.dir"));
         }
      }
   }

}
