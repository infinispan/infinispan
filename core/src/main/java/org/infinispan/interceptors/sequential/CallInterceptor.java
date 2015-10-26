package org.infinispan.interceptors.sequential;


import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.SequentialInterceptor;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Always at the end of the chain, directly in front of the cache. Simply calls into the cache using reflection.
 *
 * @author Dan Berindei
 * @since 8.1
 */
public class CallInterceptor implements SequentialInterceptor {

   private static final Log log = LogFactory.getLog(CallInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private CacheNotifier notifier;

   @Inject
   public void inject(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public CompletableFuture<Object> visitCommand(InvocationContext ctx, VisitableCommand command)
         throws Throwable {
      if (command instanceof CacheRpcCommand) {
         if (trace) log.tracef("Suppressing invocation of command %s", command.getClass());
         return null;
      } else if (command instanceof GetKeyValueCommand) {
         if (trace)
            log.trace("Executing command: " + command + ".");
         Object ret = command.perform(ctx);
         if (ret != null) {
            notifyCacheEntryVisit(ctx, ((GetKeyValueCommand) command), ((GetKeyValueCommand) command).getKey(), ret);
         }
         return ret != null ? CompletableFuture.completedFuture(ret) : null;
      } else if (command instanceof GetCacheEntryCommand) {
         if (trace)
            log.trace("Executing command: " + command + ".");
         Object ret = command.perform(ctx);
         if (ret != null) {
            notifyCacheEntryVisit(ctx, (FlagAffectedCommand) command, ((GetCacheEntryCommand) command).getKey(), ((CacheEntry) ret).getValue());
         }
         return ret != null ? CompletableFuture.completedFuture(ret) : null;
      } else if (command instanceof GetAllCommand) {
         if (trace)
            log.trace("Executing command: " + command + ".");
         Object ret = command.perform(ctx);
         if (ret != null) {
            Map<Object, Object> map = (Map<Object, Object>) ret;
            // TODO: it would be nice to know if a listener was registered for this and
            // not do the full iteration if there was no visitor listener registered
            if (((GetAllCommand) command).getFlags() == null || !((GetAllCommand) command).getFlags().contains(Flag.SKIP_LISTENER_NOTIFICATION)) {
               for (Map.Entry<Object, Object> entry : map.entrySet()) {
                  Object value = entry.getValue();
                  if (value != null) {
                     value = ((GetAllCommand) command).isReturnEntries() ? ((CacheEntry) value).getValue() : entry.getValue();
                     notifyCacheEntryVisit(ctx, ((GetAllCommand) command), entry.getKey(), value);
                  }
               }
            }
         }
         return ret != null ? CompletableFuture.completedFuture(ret) : null;
      } else {
         if (trace)
            log.trace("Executing command: " + command + ".");
         return CompletableFuture.completedFuture(command.perform(ctx));
      }
   }

   private void notifyCacheEntryVisit(InvocationContext ctx, FlagAffectedCommand command,
         Object key, Object value) {
      notifier.notifyCacheEntryVisited(key, value, true, ctx, command);
      notifier.notifyCacheEntryVisited(key, value, false, ctx, command);
   }
}