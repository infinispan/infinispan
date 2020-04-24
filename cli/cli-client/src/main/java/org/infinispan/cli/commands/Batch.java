package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.invocation.CommandInvocation;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@GroupCommandDefinition(
      name = Batch.CMD,
      description = "",
      groupCommands = {
            Add.class,
            Cache.class,
            Cas.class,
            Cd.class,
            ClearCache.class,
            Connect.class,
            Container.class,
            Counter.class,
            Create.class,
            Describe.class,
            Disconnect.class,
            Drop.class,
            Echo.class,
            Encoding.class,
            Get.class,
            Ls.class,
            Patch.class,
            Put.class,
            Query.class,
            Remove.class,
            Replace.class,
            Reset.class,
            Run.class,
            Schema.class,
            Server.class,
            Shutdown.class,
            Site.class,
            Version.class
      })
public class Batch implements Command {

   public static final String CMD = "batch";

   @Override
   public CommandResult execute(CommandInvocation invocation) {
      return CommandResult.SUCCESS;
   }
}
