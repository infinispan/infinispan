package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.terminal.utils.ANSI;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.commons.util.Util;

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
   public static final String NAMES = "names";
   public static final String PATH = "path";
   public static final String QUIET = "quiet";
   public static final String TYPE = "type";
   public static final String VALUE = "value";

   @Override
   public CommandResult execute(ContextAwareCommandInvocation invocation) throws CommandException {
      if (isHelp()) {
         invocation.println(invocation.getHelpInfo());
         return CommandResult.SUCCESS;
      }
      try {
         return exec(invocation);
      } catch (CommandException e) {
         invocation.getShell().writeln(ANSI.YELLOW_TEXT + e.getLocalizedMessage() + ANSI.DEFAULT_TEXT);
         return CommandResult.FAILURE;
      } catch (Throwable e) {
         // These are unhandled, show them in red
         Throwable cause = Util.getRootCause(e);
         invocation.getShell().writeln(ANSI.RED_TEXT + cause.getClass().getName() +": " + cause.getLocalizedMessage() + ANSI.DEFAULT_TEXT);
         return CommandResult.FAILURE;
      }
   }

   protected abstract boolean isHelp();

   protected abstract CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException;

   public int nesting() {
      return 0;
   }
}
