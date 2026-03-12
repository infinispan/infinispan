package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class CliCommand implements Command<ContextAwareCommandInvocation> {

   @Override
   public CommandResult execute(ContextAwareCommandInvocation invocation) throws CommandException {
      if (isHelp()) {
         invocation.println(invocation.getHelpInfo());
         return CommandResult.SUCCESS;
      }
      try {
         return exec(invocation);
      } catch (CommandException e) {
         Throwable cause = Util.getRootCause(e);
         invocation.errorln(e.getLocalizedMessage());
         if (cause != e) {
            invocation.errorln(cause.getClass().getSimpleName() + ": " + cause.getLocalizedMessage());
         }
         return CommandResult.FAILURE;
      } catch (CacheConfigurationException e) {
         invocation.errorln(e.getLocalizedMessage());
         return CommandResult.FAILURE;
      } catch (Throwable e) {
         // These are unhandled
         Throwable cause = Util.getRootCause(e);
         invocation.errorln(cause.getClass().getSimpleName() + ": " + cause.getLocalizedMessage());
         return CommandResult.FAILURE;
      }
   }

   protected abstract boolean isHelp();

   protected abstract CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException;

   public int nesting() {
      return 0;
   }
}
