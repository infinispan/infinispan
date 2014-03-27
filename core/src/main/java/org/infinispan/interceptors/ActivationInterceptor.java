package org.infinispan.interceptors;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ActivationInterceptor extends CacheLoaderInterceptor {

   private static final Log log = LogFactory.getLog(ActivationInterceptor.class);

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // Load the keys for our map into the data container that we are using removing them from the store.
      // This way when we overwrite the values on commit they won't be in loader and if we rollback they won't be
      // in the loader either but will be in data container at least
      for (Object key : command.getAffectedKeys()) {
         loadIfNeeded(ctx, key, false, command);
      }
      return super.visitPutMapCommand(ctx, command);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected void sendNotification(Object key, Object value, boolean pre,
                                   InvocationContext ctx, FlagAffectedCommand cmd) {
      super.sendNotification(key, value, pre, ctx, cmd);
      notifier.notifyCacheEntryActivated(key, value, pre, ctx, cmd);
   }
}


