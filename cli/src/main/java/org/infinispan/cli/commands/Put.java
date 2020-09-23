package org.infinispan.cli.commands;

import java.util.List;

import org.aesh.command.Command;
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
@MetaInfServices(Command.class)
@CommandDefinition(name = Put.CMD, description = "Puts an entry into the cache", activator = ConnectionActivator.class)
public class Put extends CliCommand {
   public static final String CMD = "put";
   public static final String ENCODING = "encoding";
   public static final String TTL = "ttl";
   public static final String MAX_IDLE = "max-idle";
   public static final String IF_ABSENT = "if-absent";

   @Arguments(required = true)
   List<String> args;

   @Option(completer = EncodingCompleter.class, shortName = 'e')
   String encoding;

   @Option(completer = CacheCompleter.class, shortName = 'c')
   String cache;

   @Option(completer = FileOptionCompleter.class, shortName = 'f')
   Resource file;

   @Option(shortName = 'l', defaultValue = "0")
   long ttl;

   @Option(name = "max-idle", shortName = 'i', defaultValue = "0")
   long maxIdle;

   @Option(name = "if-absent", shortName = 'a')
   boolean ifAbsent;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      if ((file != null) && (args.size() != 1)) {
         throw Messages.MSG.illegalCommandArguments();
      } else if ((file == null) && (args.size() != 2)) {
         throw Messages.MSG.illegalCommandArguments();
      }
      CommandInputLine cmd = new CommandInputLine(CMD)
            .arg(KEY, args.get(0))
            .optionalArg(VALUE, args.size() > 1 ? args.get(1) : null)
            .option(FILE, file != null ? file.getAbsolutePath() : null)
            .option(ENCODING, encoding)
            .option(CACHE, cache)
            .option(TTL, ttl)
            .option(MAX_IDLE, maxIdle)
            .option(IF_ABSENT, ifAbsent);
      return invocation.execute(cmd);
   }
}
