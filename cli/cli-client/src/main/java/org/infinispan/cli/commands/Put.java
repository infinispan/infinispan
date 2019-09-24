package org.infinispan.cli.commands;

import java.util.List;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.completers.EncodingCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(CliCommand.class)
@CommandDefinition(name = "put", description = "Puts an entry into the cache", activator = ConnectionActivator.class)
public class Put extends CliCommand {
   @Arguments(required = true)
   List<String> args;

   @Option(completer = EncodingCompleter.class)
   String encoding;

   @Option(completer = CacheCompleter.class)
   String cache;

   @Option(completer = FileOptionCompleter.class)
   Resource file;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      if ((file != null) && (args.size() != 1)) {
         throw Messages.MSG.illegalCommandArguments();
      } else if ((file == null) && (args.size() != 2)) {
         throw Messages.MSG.illegalCommandArguments();
      }
      CommandInputLine cmd = new CommandInputLine("put")
            .arg("key", args.get(0))
            .optionalArg("value", args.size() > 1 ? args.get(1) : null)
            .optionalArg("file", file != null ? file.getAbsolutePath() : null)
            .optionalArg("encoding", encoding)
            .optionalArg("cache", cache);
      return invocation.execute(cmd);
   }
}
