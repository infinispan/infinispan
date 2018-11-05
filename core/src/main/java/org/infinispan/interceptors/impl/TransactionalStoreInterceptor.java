package org.infinispan.interceptors.impl;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.SHARED;

import java.util.List;

import javax.transaction.Transaction;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.persistence.spi.MarshalledEntryFactory;
import org.infinispan.persistence.manager.PersistenceManager;

/**
 * An interceptor which ensures that writes to an underlying transactional store are prepared->committed/rolledback as part
 * of the 2PC, therefore ensuring that the cache and transactional store(s) remain consistent.
 *
 * @author Ryan Emerson
 * @since 9.0
 */
public class TransactionalStoreInterceptor extends DDAsyncInterceptor {
   @Inject private PersistenceManager persistenceManager;
   @Inject private InternalEntryFactory entryFactory;
   @Inject private MarshalledEntryFactory marshalledEntryFactory;

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         Transaction tx = ctx.getTransaction();
         TxBatchUpdater modBuilder = TxBatchUpdater.createTxStoreUpdater(
               persistenceManager, entryFactory, marshalledEntryFactory, ctx.getCacheTransaction().getAffectedKeys());

         List<WriteCommand> modifications = ctx.getCacheTransaction().getAllModifications();
         for (WriteCommand writeCommand : modifications) {
            writeCommand.acceptVisitor(ctx, modBuilder);
         }
         persistenceManager.prepareAllTxStores(tx, modBuilder.getModifications(), SHARED);
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         persistenceManager.commitAllTxStores(ctx.getTransaction(), SHARED);
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         persistenceManager.rollbackAllTxStores(ctx.getTransaction(), SHARED);
      }
      return invokeNext(ctx, command);
   }
}
