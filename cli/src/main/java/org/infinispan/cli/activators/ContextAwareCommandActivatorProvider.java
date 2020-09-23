package org.infinispan.cli.activators;

import java.util.Objects;

import org.aesh.command.activator.CommandActivator;
import org.aesh.command.activator.CommandActivatorProvider;
import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContextAwareCommandActivatorProvider implements CommandActivatorProvider {

   private final Context context;

   public ContextAwareCommandActivatorProvider(Context context) {
      Objects.nonNull(context);
      this.context = context;
   }

   @Override
   public CommandActivator enhanceCommandActivator(CommandActivator commandActivator) {
      if (commandActivator instanceof ContextAwareCommandActivator) {
         ((ContextAwareCommandActivator) commandActivator).setContext(context);
      }
      return commandActivator;
   }
}
