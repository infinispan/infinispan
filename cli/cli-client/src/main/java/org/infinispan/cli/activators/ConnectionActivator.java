package org.infinispan.cli.activators;

import org.aesh.command.impl.internal.ParsedCommand;
import org.infinispan.cli.Context;

public class ConnectionActivator implements ContextAwareCommandActivator {

   private Context context;

   @Override
   public boolean isActivated(ParsedCommand command) {
      return context.isConnected();
   }

   @Override
   public void setContext(Context context) {
      this.context = context;
   }
}
