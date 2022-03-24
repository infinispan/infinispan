package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.invocation.CommandInvocation;
import org.infinispan.cli.commands.rest.Add;
import org.infinispan.cli.commands.rest.Alter;
import org.infinispan.cli.commands.rest.Availability;
import org.infinispan.cli.commands.rest.Backup;
import org.infinispan.cli.commands.rest.Cas;
import org.infinispan.cli.commands.rest.ClearCache;
import org.infinispan.cli.commands.rest.Create;
import org.infinispan.cli.commands.rest.Drop;
import org.infinispan.cli.commands.rest.Get;
import org.infinispan.cli.commands.rest.Index;
import org.infinispan.cli.commands.rest.Migrate;
import org.infinispan.cli.commands.rest.Put;
import org.infinispan.cli.commands.rest.Query;
import org.infinispan.cli.commands.rest.Rebalance;
import org.infinispan.cli.commands.rest.Remove;
import org.infinispan.cli.commands.rest.Reset;
import org.infinispan.cli.commands.rest.Schema;
import org.infinispan.cli.commands.rest.Server;
import org.infinispan.cli.commands.rest.Shutdown;
import org.infinispan.cli.commands.rest.Site;
import org.infinispan.cli.commands.rest.Stats;
import org.infinispan.cli.commands.rest.Task;
import org.infinispan.cli.impl.ExitCodeResultHandler;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@GroupCommandDefinition(
      name = "batch",
      description = "",
      groupCommands = {
            Add.class,
            Alter.class,
            Availability.class,
            Backup.class,
            Cache.class,
            Cas.class,
            Cd.class,
            ClearCache.class,
            Config.class,
            Connect.class,
            Container.class,
            Counter.class,
            Create.class,
            Credentials.class,
            Describe.class,
            Disconnect.class,
            Drop.class,
            Echo.class,
            Encoding.class,
            Get.class,
            Index.class,
            Install.class,
            Ls.class,
            Migrate.class,
            Patch.class,
            Put.class,
            Query.class,
            Rebalance.class,
            Remove.class,
            Reset.class,
            Run.class,
            Schema.class,
            Server.class,
            Shutdown.class,
            Stats.class,
            Site.class,
            Task.class,
            User.class,
            Version.class
      }, resultHandler = ExitCodeResultHandler.class)
public class Batch implements Command {

   @Override
   public CommandResult execute(CommandInvocation invocation) {
      return CommandResult.SUCCESS;
   }
}
