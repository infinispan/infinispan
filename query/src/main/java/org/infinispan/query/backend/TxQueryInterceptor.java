package org.infinispan.query.backend;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.commons.util.concurrent.CompletableFutures;

public final class TxQueryInterceptor extends DDAsyncInterceptor {

   private final ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues;

   // Storing direct reference from one interceptor to another is antipattern, but extracting helper
   // wouldn't help much.
   private final QueryInterceptor queryInterceptor;

   private final InvocationSuccessFunction<VisitableCommand> commitModificationsToIndex = this::commitModificationsToIndexFuture;

   public TxQueryInterceptor(ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues, QueryInterceptor queryInterceptor) {
      this.txOldValues = txOldValues;
      this.queryInterceptor = queryInterceptor;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      if (command.isOnePhaseCommit()) {
         return invokeNextThenApply(ctx, command, commitModificationsToIndex);
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      return invokeNextThenApply(ctx, command, commitModificationsToIndex);
   }

   private InvocationStage commitModificationsToIndexFuture(InvocationContext ctx, VisitableCommand cmd, Object rv) {
      TxInvocationContext txCtx = (TxInvocationContext) ctx;
      Map<Object, Object> removed = txOldValues.remove(txCtx.getGlobalTransaction());
      final Map<Object, Object> oldValues = removed == null ? Collections.emptyMap() : removed;

      AbstractCacheTransaction transaction = txCtx.getCacheTransaction();
      return asyncValue(CompletableFuture.allOf(transaction.getAllModifications().stream()
            .filter(mod -> !mod.hasAnyFlag(FlagBitSets.SKIP_INDEXING))
            .flatMap(mod -> mod.getAffectedKeys().stream())
            .map(key -> {
               CacheEntry<?, ?> entry = txCtx.lookupEntry(key);
               if (entry != null) {
                  Object oldValue = oldValues.getOrDefault(key, QueryInterceptor.UNKNOWN);
                  return queryInterceptor.processChange(ctx, null, key, oldValue, entry.getValue());
               }
               return CompletableFutures.completedNull();
            })
            .toArray(CompletableFuture[]::new)));
   }
}
