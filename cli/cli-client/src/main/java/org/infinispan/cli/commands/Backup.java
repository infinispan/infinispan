package org.infinispan.cli.commands;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionList;
import org.aesh.io.FileResource;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.BackupCompleter;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.completers.CacheConfigurationCompleter;
import org.infinispan.cli.completers.CounterCompleter;
import org.infinispan.cli.completers.SchemaCompleter;
import org.infinispan.cli.completers.TaskCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.kohsuke.MetaInfServices;

/**
 * @author Ryan Emerson
 * @since 12.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = Backup.CMD, description = "Manages container backup creation and restoration", activator = ConnectionActivator.class,
      groupCommands = {Backup.Create.class, Backup.Delete.class, Backup.Get.class, Backup.ListBackups.class, Backup.Restore.class})
public class Backup extends CliCommand {

   public static final String CMD = "backup";
   public static final String CACHES = "caches";
   public static final String CACHE_CONFIGS = "cache-configs";
   public static final String COUNTERS = "counters";
   public static final String PROTO_SCHEMAS = "proto-schemas";
   public static final String SCRIPTS = "scripts";
   public static final String[] ALL_RESOURCE_TYPES = new String[]{CACHES, CACHE_CONFIGS, COUNTERS, PROTO_SCHEMAS, SCRIPTS};

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

   public static Map<String, List<String>> createResourceMap(CommandInputLine cmd) {
      Map<String, List<String>> resourceMap = new HashMap<>();
      for (String resource : Backup.ALL_RESOURCE_TYPES) {
         if (cmd.hasArg(resource)) {
            resourceMap.put(resource, cmd.argAs(resource));
         }
      }
      return resourceMap;
   }

   @CommandDefinition(name = Delete.CMD, description = "Delete a backup on the server", activator = ConnectionActivator.class)
   public static class Delete extends AbstractBackupCommand {
      public static final String CMD = "delete";

      @Argument(description = "The name of the backup", completer = BackupCompleter.class, required = true)
      String name;

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) {
         return invocation.execute(
               new CommandInputLine(Backup.CMD)
                     .arg(TYPE, CMD)
                     .arg(NAME, name)
         );
      }
   }

   @CommandDefinition(name = Get.CMD, description = "Get a backup from the server", activator = ConnectionActivator.class)
   public static class Get extends AbstractBackupCommand {
      public static final String CMD = "get";
      public static final String NO_CONTENT = "no-content";

      @Argument(description = "The name of the backup", completer = BackupCompleter.class, required = true)
      String name;

      @Option(description = "No content is downloaded, but the command only returns once the backup has finished", hasValue = false, name = NO_CONTENT)
      boolean noContent;

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) {
         return invocation.execute(
               new CommandInputLine(Backup.CMD)
                     .arg(TYPE, CMD)
                     .arg(NAME, name)
                     .option(NO_CONTENT, noContent)
         );
      }
   }

   @CommandDefinition(name = ListBackups.CMD, description = "List all backups on the server", activator = ConnectionActivator.class)
   public static class ListBackups extends AbstractBackupCommand {
      public static final String CMD = "ls";

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) {
         return invocation.execute(
               new CommandInputLine(Backup.CMD)
                     .arg(TYPE, CMD)
         );
      }
   }

   @CommandDefinition(name = Create.CMD, description = "Create a backup on the server", activator = ConnectionActivator.class)
   public static class Create extends AbstractResourceCommand {
      public static final String CMD = "create";
      public static final String DIR = "dir";

      @Option(shortName = 'd', description = "The directory on the server to be used for creating and storing the backup")
      String dir;

      @Option(shortName = 'n', description = "The name of the backup")
      String name;

      public Create() {
         super(CMD);
      }

      @Override
      protected void additionalArgs(CommandInputLine cmd) {
         cmd.option(DIR, dir)
               .option(NAME, name);
      }
   }

   @CommandDefinition(name = Restore.CMD, description = "Restore a backup", activator = ConnectionActivator.class)
   public static class Restore extends AbstractResourceCommand {
      public static final String CMD = "restore";
      public static final String UPLOAD_BACKUP = "upload-backup";

      @Argument(description = "The path of the backup file ", completer = FileOptionCompleter.class, required = true)
      Resource path;

      @Option(shortName = 'u', description = "Indicates that the path is a local file which must be uploaded to the server", hasValue = false, name = UPLOAD_BACKUP)
      boolean upload;

      public Restore() {
         super(CMD);
      }

      @Override
      protected void additionalArgs(CommandInputLine cmd) {
         if (!upload && !((FileResource) path).getFile().isAbsolute())
            throw Messages.MSG.backupAbsolutePathRequired();

         cmd.arg(PATH, path)
               .optionalArg(UPLOAD_BACKUP, upload);
      }
   }

   private abstract static class AbstractBackupCommand extends CliCommand {
      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }
   }

   private abstract static class AbstractResourceCommand extends AbstractBackupCommand {
      final String type;

      @OptionList(description = "Comma separated list of caches to include, '*' indicates all available",
            completer = CacheCompleter.class, name = CACHES)
      List<String> caches;

      @OptionList(description = "Comma separated list of cache configurations to include, '*' indicates all available",
            completer = CacheConfigurationCompleter.class, name = CACHE_CONFIGS)
      List<String> cacheConfigs;

      @OptionList(description = "Comma separated list of counters to include, '*' indicates all available",
            completer = CounterCompleter.class, name = COUNTERS)
      List<String> counters;

      @OptionList(description = "Comma separated list of proto schemas to include, '*' indicates all available",
            completer = SchemaCompleter.class, name = PROTO_SCHEMAS)
      List<String> protoSchemas;

      @OptionList(description = "Comma separated list of scripts to include, '*' indicates all available",
            completer = TaskCompleter.class, name = SCRIPTS)
      List<String> scripts;

      AbstractResourceCommand(String type) {
         this.type = type;
      }

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(Backup.CMD)
               .arg(TYPE, type)
               .optionalArg(CACHES, caches)
               .optionalArg(CACHE_CONFIGS, cacheConfigs)
               .optionalArg(COUNTERS, counters)
               .optionalArg(PROTO_SCHEMAS, protoSchemas)
               .optionalArg(SCRIPTS, scripts);
         additionalArgs(cmd);
         return invocation.execute(cmd);
      }

      abstract void additionalArgs(CommandInputLine cmd);
   }
}
