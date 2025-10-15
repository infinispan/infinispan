package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.Collection;

import org.aesh.command.Command;
import org.infinispan.cli.Context;

public class PersistentScopeCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      throw new UnsupportedOperationException();
   }

   @Override
   protected Collection<String> getAvailableItems(ContextAwareCompleterInvocation invocation) throws IOException {
      Command<?> command = invocation.getCommand();
      if (command instanceof PersistentScopeAwareCommand psac) {
         return psac.getPersistentScopes();
      }
      return super.getAvailableItems(invocation);
   }

   public interface PersistentScopeAwareCommand {
      Collection<String> getPersistentScopes();
   }
}
