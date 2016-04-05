package org.infinispan.interceptors.impl;


import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDSequentialInterceptor;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Always at the end of the chain, directly in front of the cache. Simply calls into the cache using reflection. If the
 * call resulted in a modification, add the Modification to the end of the modification list keyed by the current
 * transaction.
 *
 *
 * @author Bela Ban
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 9.0
 */
public class CallInterceptor extends DDSequentialInterceptor {
   // TODO Invoke the command directly in BaseSequentialInvocationChain#invokeNext and remove this interceptor?
   private static final Log log = LogFactory.getLog(CallInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private CacheNotifier notifier;

   @Inject
   public void inject(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handlePrepareCommand.");
      return ctx.shortCircuit(null);
   }

   @Override
   public CompletableFuture<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handleCommitCommand.");
      return ctx.shortCircuit(null);
   }

   @Override
   public CompletableFuture<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handleRollbackCommand.");
      return ctx.shortCircuit(null);
   }

   @Override
   public CompletableFuture<Void> visitLockControlCommand(TxInvocationContext ctx, LockControlCommand c) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handleLockControlCommand.");
      return ctx.shortCircuit(null);
   }

   @Override
   public CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      if (trace) log.trace("Executing command: " + command + ".");
      Object ret = command.perform(ctx);
      if (ret != null) {
         notifyCacheEntryVisit(ctx, command, command.getKey(), ret);
      }
      return ctx.shortCircuit(ret);
   }

   @Override
   public CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      if (trace) log.trace("Executing command: " + command + ".");
      Object ret = command.perform(ctx);
      if (ret != null) {
         notifyCacheEntryVisit(ctx, command, command.getKey(), ((CacheEntry) ret).getValue());
      }
      return ctx.shortCircuit(ret);
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      if (trace) log.trace("Executing command: " + command + ".");
      Object ret = command.perform(ctx);
      if (ret != null) {
         Map<Object, Object> map = (Map<Object, Object>) ret;
         // TODO: it would be nice to know if a listener was registered for this and
         // not do the full iteration if there was no visitor listener registered
         if (command.getFlags() == null || !command.getFlags().contains(Flag.SKIP_LISTENER_NOTIFICATION)) {
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
               Object value = entry.getValue();
               if (value != null) {
                  value = command.isReturnEntries() ? ((CacheEntry) value).getValue() : entry.getValue();
                  notifyCacheEntryVisit(ctx, command, entry.getKey(), value);
               }
            }
         }
      }
      return ctx.shortCircuit(ret);
   }

   private void notifyCacheEntryVisit(InvocationContext ctx, FlagAffectedCommand command,
         Object key, Object value) {
      notifier.notifyCacheEntryVisited(key, value, true, ctx, command);
      notifier.notifyCacheEntryVisited(key, value, false, ctx, command);
   }

   @Override
   final public CompletableFuture<Void> handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (trace) log.trace("Executing command: " + command + ".");
      return ctx.shortCircuit(command.perform(ctx));
   }
}