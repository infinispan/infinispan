package org.infinispan.interceptors.impl;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.conflict.impl.SegmentHashTracker;
import org.infinispan.conflict.impl.SegmentHasher;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.persistence.manager.PersistenceManager;

public class SegmentHashInterceptor extends DDAsyncInterceptor {

   @Inject SegmentHashTracker tracker;
   @Inject PersistenceManager persistenceManager;
   @Inject KeyPartitioner keyPartitioner;
   @Inject DistributionManager distributionManager;

   private boolean hasStores;

   private final InvocationSuccessFunction<DataWriteCommand> singleKeyHandler = this::handleSingleKeyWrite;

   @Start
   public void start() {
      hasStores = tracker.hasStores();
   }

   // Single-key write commands

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleSingleKey(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleSingleKey(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleSingleKey(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleSingleKey(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleSingleKey(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleSingleKey(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleSingleKey(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleSingleKey(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleSingleKey(ctx, command);
   }

   @Override
   public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) throws Throwable {
      return handleSingleKey(ctx, command);
   }

   // Multi-key write commands

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleMultiKey(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleMultiKey(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleMultiKey(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleMultiKey(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleMultiKey(ctx, command);
   }

   // Clear

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> tracker.resetAllSegments());
   }

   // Transaction support

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!command.isOnePhaseCommit()) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) ->
            processTransactionModifications((TxInvocationContext) rCtx));
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) ->
            processTransactionModifications((TxInvocationContext) rCtx));
   }

   // Internal handlers

   private Object handleSingleKey(InvocationContext ctx, DataWriteCommand command) {
      if (ctx.isInTxScope()) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenApply(ctx, command, singleKeyHandler);
   }

   private Object handleSingleKeyWrite(InvocationContext ctx, DataWriteCommand command, Object rv) {
      Object key = command.getKey();
      CacheEntry<?, ?> entry = ctx.lookupEntry(key);
      if (entry == null) return rv;

      int segment = SegmentSpecificCommand.extractSegment(command, key, keyPartitioner);
      return processEntry(key, entry, segment, rv);
   }

   private Object handleMultiKey(InvocationContext ctx, WriteCommand command) {
      if (ctx.isInTxScope()) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         for (Object key : rCommand.getAffectedKeys()) {
            CacheEntry<?, ?> entry = rCtx.lookupEntry(key);
            if (entry == null) continue;
            int segment = keyPartitioner.getSegment(key);
            processEntry(key, entry, segment, null);
         }
      });
   }

   @SuppressWarnings("rawtypes")
   private void processTransactionModifications(TxInvocationContext<?> ctx) {
      for (Map.Entry<Object, CacheEntry> e : ctx.getLookedUpEntries().entrySet()) {
         Object key = e.getKey();
         CacheEntry<?, ?> entry = e.getValue();
         if (entry == null) continue;
         int segment = keyPartitioner.getSegment(key);
         processEntry(key, entry, segment, null);
      }
   }

   private Object processEntry(Object key, CacheEntry<?, ?> entry, int segment, Object rv) {
      if (entry.isRemoved()) {
         return handleRemoval(key, entry, segment, rv);
      } else if (entry.isChanged()) {
         return handleWrite(key, entry, segment, rv);
      }
      return rv;
   }

   private Object handleRemoval(Object key, CacheEntry<?, ?> entry, int segment, Object rv) {
      Object oldValue = getOldValue(entry);
      if (oldValue != null) {
         SegmentHasher.HashAndBucket hb = tracker.computeHashAndBucket(key, oldValue);
         tracker.recordRemove(segment, hb.bucket(), hb.hash());
         return rv;
      } else if (hasStores) {
         return delayedValue(loadOldValueAndRemove(key, segment), rv);
      }
      return rv;
   }

   private Object handleWrite(Object key, CacheEntry<?, ?> entry, int segment, Object rv) {
      Object oldValue = getOldValue(entry);
      Object newValue = entry.getValue();

      if (oldValue != null) {
         // Compute the key hash once; reuse it for both old and new entry hashes to avoid
         // marshalling the key twice.
         long keyHash = SegmentHasher.hashObject(key, tracker.marshaller());
         SegmentHasher.HashAndBucket oldHb = tracker.computeHashAndBucket(keyHash, oldValue);
         SegmentHasher.HashAndBucket newHb = tracker.computeHashAndBucket(keyHash, newValue);
         tracker.recordUpdate(segment, oldHb.bucket(), oldHb.hash(), newHb.hash());
         return rv;
      } else if (hasStores) {
         return delayedValue(loadOldValueAndUpdate(key, newValue, segment), rv);
      } else {
         SegmentHasher.HashAndBucket hb = tracker.computeHashAndBucket(key, newValue);
         tracker.recordInsert(segment, hb.bucket(), hb.hash());
         return rv;
      }
   }

   private Object getOldValue(CacheEntry<?, ?> entry) {
      if (entry instanceof MVCCEntry<?, ?> mvcc) {
         return mvcc.getOldValue();
      }
      return null;
   }

   private CompletionStage<Void> loadOldValueAndRemove(Object key, int segment) {
      return persistenceManager.loadFromAllStores(key, segment, false, true)
            .thenAccept(loaded -> {
               if (loaded != null) {
                  SegmentHasher.HashAndBucket hb = tracker.computeHashAndBucket(key, loaded.getValue());
                  tracker.recordRemove(segment, hb.bucket(), hb.hash());
               }
            });
   }

   private CompletionStage<Void> loadOldValueAndUpdate(Object key, Object newValue, int segment) {
      return persistenceManager.loadFromAllStores(key, segment, false, true)
            .thenAccept(loaded -> {
               long keyHash = SegmentHasher.hashObject(key, tracker.marshaller());
               if (loaded != null) {
                  SegmentHasher.HashAndBucket oldHb = tracker.computeHashAndBucket(keyHash, loaded.getValue());
                  SegmentHasher.HashAndBucket newHb = tracker.computeHashAndBucket(keyHash, newValue);
                  tracker.recordUpdate(segment, oldHb.bucket(), oldHb.hash(), newHb.hash());
               } else {
                  SegmentHasher.HashAndBucket hb = tracker.computeHashAndBucket(keyHash, newValue);
                  tracker.recordInsert(segment, hb.bucket(), hb.hash());
               }
            });
   }
}
