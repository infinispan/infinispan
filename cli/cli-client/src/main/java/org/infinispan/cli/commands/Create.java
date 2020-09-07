package org.infinispan.cli.commands;

import java.util.Collections;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CacheConfigurationCompleter;
import org.infinispan.cli.completers.CounterStorageCompleter;
import org.infinispan.cli.completers.CounterTypeCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = Create.CMD, description = "Creates a cache or a counter", activator = ConnectionActivator.class, groupCommands = {Create.Cache.class, Create.Counter.class})
public class Create extends CliCommand {

   public static final String CMD = "create";
   public static final String TYPE = "type";
   public static final String NAME = "name";

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

   @CommandDefinition(name = Cache.CMD, description = "Create a cache", activator = ConnectionActivator.class)
   public static class Cache extends CliCommand {
      public static final String CMD = "cache";
      public static final String TEMPLATE = "template";
      public static final String FILE = "file";
      public static final String VOLATILE = "volatile";

      @Argument(required = true)
      String name;

      @Option(completer = CacheConfigurationCompleter.class, shortName = 't')
      String template;

      @Option(completer = FileOptionCompleter.class, shortName = 'f')
      Resource file;

      @Option(defaultValue = "false", name = "volatile", shortName = 'v')
      boolean volatileCache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         if (template != null && file != null) {
            throw Messages.MSG.mutuallyExclusiveOptions("template", "file");
         }
         if (template == null && file == null) {
            throw Messages.MSG.requiresOneOf("template", "file");
         }
         CommandInputLine cmd = new CommandInputLine(Create.CMD)
               .arg(TYPE, Cache.CMD)
               .arg(NAME, name)
               .optionalArg(TEMPLATE, template)
               .optionalArg(FILE, file != null ? file.getAbsolutePath() : null)
               .option(VOLATILE, volatileCache);
         return invocation.execute(cmd);
      }
   }

   @CommandDefinition(name = Counter.CMD, description = "Create a counter", activator = ConnectionActivator.class)
   public static class Counter extends CliCommand {
      public static final String CMD = "counter";
      public static final String COUNTER_TYPE = "counter-type";
      public static final String INITIAL_VALUE = "initial-value";
      public static final String STORAGE = "storage";
      public static final String UPPER_BOUND = "upper-bound";
      public static final String LOWER_BOUND = "lower-bound";
      public static final String CONCURRENCY_LEVEL = "concurrency-level";

      @Argument(required = true)
      String name;

      @Option(shortName = 't', defaultValue = "", completer = CounterTypeCompleter.class, description = "Type of counter [weak|strong]")
      String type;

      @Option(shortName = 'i', name = "initial-value", defaultValue = "0", description = "Initial value for the counter (defaults to 0)")
      Long initialValue;

      @Option(shortName = 's', defaultValue = "VOLATILE", completer = CounterStorageCompleter.class, description = "persistent state PERSISTENT | VOLATILE (default)")
      String storage;

      @Option(shortName = 'u', name = "upper-bound")
      Long upperBound;

      @Option(shortName = 'l', name = "lower-bound")
      Long lowerBound;

      @Option(shortName = 'c', name = "concurrency-level", defaultValue = "16", description = "concurrency level for weak counters, defaults to 16")
      Integer concurrencyLevel;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(Create.CMD)
               .arg(Create.TYPE, Counter.CMD)
               .arg(Create.NAME, name)
               .option(COUNTER_TYPE, type)
               .option(INITIAL_VALUE, initialValue)
               .option(STORAGE, storage)
               .option(UPPER_BOUND, upperBound)
               .option(LOWER_BOUND, lowerBound)
               .option(CONCURRENCY_LEVEL, concurrencyLevel);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }
}
