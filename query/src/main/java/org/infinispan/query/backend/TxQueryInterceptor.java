package org.infinispan.query.backend;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

public class TxQueryInterceptor extends DDAsyncInterceptor {
   private final ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues;
   // Storing direct reference from one interceptor to another is antipattern, but extracting helper
   // wouldn't help much.
   private final QueryInterceptor queryInterceptor;

   private final InvocationSuccessAction commitModificationsToIndex = this::commitModificationsToIndex;

   public TxQueryInterceptor(ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues, QueryInterceptor queryInterceptor) {
      this.txOldValues = txOldValues;
      this.queryInterceptor = queryInterceptor;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (command.isOnePhaseCommit()) {
         return invokeNextThenAccept(ctx, command, commitModificationsToIndex);
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, commitModificationsToIndex);
   }

   private void commitModificationsToIndex(InvocationContext ctx, VisitableCommand cmd, Object rv) {
      TxInvocationContext txCtx = (TxInvocationContext) ctx;
      Map<Object, Object> oldValues = txOldValues.remove(txCtx.getGlobalTransaction());
      if (oldValues == null) {
         oldValues = Collections.emptyMap();
      }
      AbstractCacheTransaction transaction = txCtx.getCacheTransaction();
      Set<Object> keys = transaction.getAffectedKeys();
      if (!ctx.isOriginLocal() || transaction.getModifications().stream().anyMatch(mod -> mod.hasAnyFlag(FlagBitSets.SKIP_INDEXING))) {
         keys = transaction.getModifications().stream()
               .filter(mod -> !mod.hasAnyFlag(FlagBitSets.SKIP_INDEXING))
               .flatMap(mod -> mod.getAffectedKeys().stream())
               .collect(Collectors.toSet());
      }
      for (Object key : keys) {
         CacheEntry entry = txCtx.lookupEntry(key);
         if (entry != null) {
            Object oldValue = oldValues.getOrDefault(key, QueryInterceptor.UNKNOWN);
            queryInterceptor.processChange(ctx, null, key, oldValue, entry.getValue(), NoTransactionContext.INSTANCE);
         }
      }
   }
}
