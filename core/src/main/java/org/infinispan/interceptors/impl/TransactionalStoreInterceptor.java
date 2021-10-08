package org.infinispan.interceptors.impl;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntryFactory;

/**
 * An interceptor which ensures that writes to an underlying transactional store are prepared->committed/rolledback as part
 * of the 2PC, therefore ensuring that the cache and transactional store(s) remain consistent.
 *
 * @author Ryan Emerson
 * @since 9.0
 */
public class TransactionalStoreInterceptor extends DDAsyncInterceptor {
   @Inject PersistenceManager persistenceManager;
   @Inject InternalEntryFactory entryFactory;
   @Inject MarshallableEntryFactory marshalledEntryFactory;

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      if (ctx.isOriginLocal()) {
         if (!command.isOnePhaseCommit()) {
            return asyncInvokeNext(ctx, command, persistenceManager.prepareAllTxStores(ctx, BOTH));
         } else {
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
               if (command.isSuccessful())
                  return null;

               // Persist the modifications in one phase
               // After they were successfully applied in the data container
               return asyncValue(persistenceManager.performBatch(ctx, (writeCommand, o) -> true));
            });
         }
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      if (ctx.isOriginLocal()) {
         return asyncInvokeNext(ctx, command, persistenceManager.commitAllTxStores(ctx, BOTH));
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
      if (ctx.isOriginLocal()) {
         return asyncInvokeNext(ctx, command, persistenceManager.rollbackAllTxStores(ctx, BOTH));
      }
      return invokeNext(ctx, command);
   }
}
