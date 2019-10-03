package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class CliCommand implements Command<ContextAwareCommandInvocation> {
   public static final String CACHE = "cache";
   public static final String COUNTER = "counter";
   public static final String FILE = "file";
   public static final String KEY = "key";
   public static final String NAME = "name";
   public static final String PATH = "path";
   public static final String QUIET = "quiet";
   public static final String TYPE = "type";
   public static final String VALUE = "value";

   @Option(shortName = 'h', hasValue = false)
   protected boolean help;

   @Override
   public CommandResult execute(ContextAwareCommandInvocation invocation) throws CommandException {
      if (help) {
         invocation.println(invocation.getHelpInfo());
         return CommandResult.SUCCESS;
      }
      return exec(invocation);
   }

   protected abstract CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException;

   public int nesting() {
      return 0;
   }
}
