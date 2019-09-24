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
