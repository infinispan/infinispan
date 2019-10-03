package org.infinispan.cli.completers;

import java.util.ArrayList;
import java.util.List;

import org.aesh.command.completer.CompleterInvocation;
import org.aesh.command.completer.OptionCompleter;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.registry.CommandRegistry;

public class HelpCompleter implements OptionCompleter {
   @Override
   public void complete(CompleterInvocation invocation) {
      List<String> completeValues = new ArrayList<>();
      CommandRegistry<? extends CommandInvocation> registry = ((ContextAwareCompleterInvocation) invocation).context.getRegistry();
      if (registry != null) {
         for (String command : registry.getAllCommandNames()) {
            if (command.startsWith(invocation.getGivenCompleteValue()))
               completeValues.add(command);
         }
         invocation.setCompleterValues(completeValues);
      }
   }
}
