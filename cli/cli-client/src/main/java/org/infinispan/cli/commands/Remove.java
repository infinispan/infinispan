package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Remove.CMD, description = "Removes an entry from the cache", aliases = "rm", activator = ConnectionActivator.class)
public class Remove extends CliCommand {
   public static final String CMD = "remove";
   @Argument(required = true)
   String key;

   @Option(completer = CacheCompleter.class)
   String cache;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      CommandInputLine cmd = new CommandInputLine(CMD).arg("key", key).optionalArg("cache", cache);
      return invocation.execute(cmd);
   }
}
