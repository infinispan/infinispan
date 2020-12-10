package org.infinispan.cli.commands.rest;

import java.io.File;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionList;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.BackupCompleter;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.completers.CacheConfigurationCompleter;
import org.infinispan.cli.completers.CounterCompleter;
import org.infinispan.cli.completers.SchemaCompleter;
import org.infinispan.cli.completers.TaskCompleter;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.util.Version;
import org.kohsuke.MetaInfServices;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * @author Ryan Emerson
 * @since 12.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "backup", description = "Manages container backup creation and restoration", activator = ConnectionActivator.class,
      groupCommands = {Backup.Create.class, Backup.Delete.class, Backup.Get.class, Backup.ListBackups.class, Backup.Restore.class})
public class Backup extends CliCommand {

   public static final String CACHES = "caches";
   public static final String TEMPLATES = "templates";
   public static final String COUNTERS = "counters";
   public static final String PROTO_SCHEMAS = "proto-schemas";
   public static final String TASKS = "tasks";

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

   @CommandDefinition(name = "delete", description = "Delete a backup on the server", activator = ConnectionActivator.class)
   public static class Delete extends AbstractBackupCommand {

      @Argument(description = "The name of the backup", completer = BackupCompleter.class, required = true)
      String name;

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         String container = invocation.getContext().getConnection().getActiveContainer().getName();
         invocation.printf("Deleting backup %s%n", name);
         return client.cacheManager(container).deleteBackup(this.name);
      }
   }

   @CommandDefinition(name = "get", description = "Get a backup from the server", activator = ConnectionActivator.class)
   public static class Get extends AbstractBackupCommand {
      public static final String NO_CONTENT = "no-content";

      @Argument(description = "The name of the backup", completer = BackupCompleter.class, required = true)
      String name;

      @Option(description = "No content is downloaded, but the command only returns once the backup has finished", hasValue = false, name = NO_CONTENT)
      boolean noContent;

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         String container = invocation.getContext().getConnection().getActiveContainer().getName();
         invocation.printf("Downloading backup %s%n", name);
         // Poll the backup's availability every 500 milliseconds with a maximum of 100 attempts
         return Flowable.timer(500, TimeUnit.MILLISECONDS, Schedulers.trampoline())
               .repeat(100)
               .flatMapSingle(Void -> Single.fromCompletionStage(client.cacheManager(container).getBackup(name, noContent)))
               .takeUntil(rsp -> rsp.getStatus() != 202)
               .lastOrErrorStage();
      }

      @Override
      public Connection.ResponseMode getResponseMode() {
         return noContent ? Connection.ResponseMode.QUIET : Connection.ResponseMode.FILE;
      }
   }

   @CommandDefinition(name = "ls", description = "List all backups on the server", activator = ConnectionActivator.class)
   public static class ListBackups extends AbstractBackupCommand {

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         String container = invocation.getContext().getConnection().getActiveContainer().getName();
         return client.cacheManager(container).getBackupNames();
      }
   }

   @CommandDefinition(name = "create", description = "Create a backup on the server", activator = ConnectionActivator.class)
   public static class Create extends AbstractResourceCommand {

      @Option(shortName = 'd', description = "The directory on the server to be used for creating and storing the backup")
      String dir;

      @Option(shortName = 'n', description = "The name of the backup")
      String name;

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         // If the backup name has not been specified generate one based upon the Infinispan version and timestamp
         String backupName = name != null ? name : String.format("%s-%tY%2$tm%2$td%2$tH%2$tM%2$tS", Version.getBrandName(), LocalDateTime.now());
         invocation.printf("Creating backup '%s'%n", backupName);
         String container = invocation.getContext().getConnection().getActiveContainer().getName();
         return client.cacheManager(container).createBackup(backupName, dir, createResourceMap());
      }
   }

   @CommandDefinition(name = "restore", description = "Restore a backup", activator = ConnectionActivator.class)
   public static class Restore extends AbstractResourceCommand {

      @Argument(description = "The path of the backup file ", completer = FileOptionCompleter.class, required = true)
      Resource path;

      @Option(shortName = 'n', description = "Defines a name for the restore request.")
      String name;

      @Option(shortName = 'u', description = "Indicates that the path is a local file which must be uploaded to the server", hasValue = false, name = "upload-backup")
      boolean upload;

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
         Map<String, List<String>> resources = createResourceMap();
         // If the restore name has not been specified generate one based upon the Infinispan version and timestamp
         String restoreName = name != null ? name : String.format("%s-%tY%2$tm%2$td%2$tH%2$tM%2$tS", Version.getBrandName(), LocalDateTime.now());
         String container = invocation.getContext().getConnection().getActiveContainer().getName();
         if (upload) {
            invocation.printf("Uploading backup '%s' and restoring%n", path.getAbsolutePath());
            File file = new File(path.getAbsolutePath());
            return client.cacheManager(container).restore(restoreName, file, resources).thenCompose(rsp -> pollRestore(restoreName, container, client, rsp));
         } else {
            invocation.printf("Restoring from backup '%s'%n", path.getAbsolutePath());
            return client.cacheManager(container).restore(restoreName, path.getAbsolutePath(), resources).thenCompose(rsp -> pollRestore(restoreName, container, client, rsp));
         }
      }
   }

   private static CompletionStage<RestResponse> pollRestore(String restoreName, String container, RestClient c, RestResponse rsp) {
      if (rsp.getStatus() != 202) {
         return CompletableFuture.completedFuture(rsp);
      }
      // Poll the restore progress every 500 milliseconds with a maximum of 100 attempts
      return Flowable.timer(500, TimeUnit.MILLISECONDS, Schedulers.trampoline())
            .repeat(100)
            .flatMapSingle(Void -> Single.fromCompletionStage(c.cacheManager(container).getRestore(restoreName)))
            .takeUntil(r -> r.getStatus() != 202)
            .lastOrErrorStage();
   }

   private abstract static class AbstractBackupCommand extends RestCliCommand {
      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }
   }

   private abstract static class AbstractResourceCommand extends AbstractBackupCommand {
      @OptionList(description = "Comma separated list of caches to include, '*' indicates all available",
            completer = CacheCompleter.class, name = CACHES)
      List<String> caches;

      @OptionList(description = "Comma separated list of cache templates to include, '*' indicates all available",
            completer = CacheConfigurationCompleter.class, name = TEMPLATES)
      List<String> templates;

      @OptionList(description = "Comma separated list of counters to include, '*' indicates all available",
            completer = CounterCompleter.class, name = COUNTERS)
      List<String> counters;

      @OptionList(description = "Comma separated list of proto schemas to include, '*' indicates all available",
            completer = SchemaCompleter.class, name = PROTO_SCHEMAS)
      List<String> protoSchemas;

      @OptionList(description = "Comma separated list of tasks to include, '*' indicates all available",
            completer = TaskCompleter.class, name = TASKS)
      List<String> tasks;

      public Map<String, List<String>> createResourceMap() {
         Map<String, List<String>> resourceMap = new HashMap<>();
         if (caches != null) {
            resourceMap.put(CACHES, caches);
         }
         if (templates != null) {
            resourceMap.put(TEMPLATES, templates);
         }
         if (counters != null) {
            resourceMap.put(COUNTERS, counters);
         }
         if (protoSchemas != null) {
            resourceMap.put(PROTO_SCHEMAS, protoSchemas);
         }
         if (tasks != null) {
            resourceMap.put(TASKS, tasks);
         }
         return resourceMap;
      }
   }
}
