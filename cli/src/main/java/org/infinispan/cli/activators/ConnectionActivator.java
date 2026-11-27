package org.infinispan.cli.activators;

import org.aesh.command.impl.internal.ParsedCommand;
import org.infinispan.cli.Context;

public class ConnectionActivator implements ContextAwareCommandActivator {

   private Context context;

   @Override
   public boolean isActivated(ParsedCommand parsedCommand) {
      // In batch mode or when no connection required, context may be null
      return context == null || context.isConnected();
   }

   @Override
   public void setContext(Context context) {
      this.context = context;
   }
}
