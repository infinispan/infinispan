package org.infinispan.interceptors.impl;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.SHARED;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.transaction.Transaction;

import org.infinispan.atomic.impl.AtomicHashMap;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.DeltaAwareCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.support.BatchModification;

/**
 * An interceptor which ensures that writes to an underlying transactional store are prepared->committed/rolledback as part
 * of the 2PC, therefore ensuring that the cache and transactional store(s) remain consistent.
 *
 * @author Ryan Emerson
 * @since 9.0
 */
public class TransactionalStoreInterceptor extends DDAsyncInterceptor {

   private PersistenceManager persistenceManager;
   private InternalEntryFactory entryFactory;
   private StreamingMarshaller marshaller;

   @Inject
   protected void init(PersistenceManager persistenceManager, InternalEntryFactory entryFactory,
                       StreamingMarshaller marshaller) {
      this.persistenceManager = persistenceManager;
      this.entryFactory = entryFactory;
      this.marshaller = marshaller;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         Transaction tx = ctx.getTransaction();
         Updater modBuilder = new Updater(ctx.getCacheTransaction().getAffectedKeys());
         List<WriteCommand> modifications = ctx.getCacheTransaction().getAllModifications();
         for (WriteCommand writeCommand : modifications) {
            writeCommand.acceptVisitor(ctx, modBuilder);
         }
         persistenceManager.prepareAllTxStores(tx, modBuilder.modifications, SHARED);
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

   private class Updater extends AbstractVisitor {
      private final BatchModification modifications;

      Updater(Set<Object> affectedKeys) {
         modifications = new BatchModification(affectedKeys);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return visitSingleStore(ctx, command.getKey());
      }

      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         CacheEntry entry = ctx.lookupEntry(command.getKey());
         InternalCacheEntry ice;
         if (entry instanceof InternalCacheEntry) {
            ice = (InternalCacheEntry) entry;
         } else if (entry instanceof DeltaAwareCacheEntry) {
            AtomicHashMap<?, ?> uncommittedChanges = ((DeltaAwareCacheEntry) entry).getUncommittedChages();
            ice = entryFactory.create(entry.getKey(), uncommittedChanges, entry.getMetadata(), entry.getLifespan(), entry.getMaxIdle());
         } else {
            ice = entryFactory.create(entry);
         }
         MarshalledEntryImpl marshalledEntry = new MarshalledEntryImpl(ice.getKey(), ice.getValue(), PersistenceUtil.internalMetadata(ice), marshaller);
         modifications.addMarshalledEntry(ice.getKey(), marshalledEntry);
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         return visitSingleStore(ctx, command.getKey());
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         Map<Object, Object> map = command.getMap();
         for (Object key : map.keySet())
            visitSingleStore(ctx, key);
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         modifications.removeEntry(command.getKey());
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         // This should never happen as the ClearCommand should never be included in a Tx's modification list
         throw new UnsupportedOperationException("Clear command not supported inside a transaction");
      }

      private Object visitSingleStore(InvocationContext ctx, Object key) throws Throwable {
         InternalCacheValue icv = entryFactory.getValueFromCtxOrCreateNew(key, ctx);
         modifications.addMarshalledEntry(key, new MarshalledEntryImpl(key, icv.getValue(), PersistenceUtil.internalMetadata(icv), marshaller));
         return null;
      }
   }
}
