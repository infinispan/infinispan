package org.infinispan.partitionhandling.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.RpcException;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PartitionHandlingInterceptor extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(PartitionHandlingInterceptor.class);

   @Inject private PartitionHandlingManager partitionHandlingManager;

   private InvocationFinallyAction handleDataReadReturn = this::handleDataReadReturn;
   private InvocationFinallyAction handleGetAllCommandReturn = this::handleGetAllCommandReturn;
   private InvocationSuccessAction postTxCommandCheck = this::postTxCommandCheck;

   private boolean performPartitionCheck(InvocationContext ctx, FlagAffectedCommand command) {
      return !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.PUT_FOR_STATE_TRANSFER);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   protected Object handleSingleWrite(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkWrite(command.getKey());
      }
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         for (Object k : command.getAffectedKeys())
            partitionHandlingManager.checkWrite(k);
      }
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkClear();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkBulkRead();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkBulkRead();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      return handleDataReadCommand(ctx, command);
   }

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return handleDataReadCommand(ctx, command);
   }

   private Object handleDataReadCommand(InvocationContext ctx, DataCommand command) {
      return invokeNextAndFinally(ctx, command, handleDataReadReturn);
   }

   private void handleDataReadReturn(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable t) throws Throwable {
      DataCommand dataCommand = (DataCommand) rCommand;
      if (!performPartitionCheck(rCtx, dataCommand))
         return;

      if (t != null) {
         if (t instanceof RpcException) {
            // We must have received an AvailabilityException from one of the owners.
            // There is no way to verify the cause here, but there isn't any other way to get an invalid
            // get response.
            throw log.degradedModeKeyUnavailable(dataCommand.getKey());
         } else if (t instanceof AllOwnersLostException) {
            // Scattered cache throws AllOwnersLostException even if there's no need to fail with AvailabilityException\
            // Dist caches should never throw AllOwnersLostException
            assert cacheConfiguration.clustering().cacheMode().isScattered();
         }
      }

      // We do the availability check after the read, because the cache may have entered degraded mode
      // while we were reading from a remote node.
      partitionHandlingManager.checkRead(dataCommand.getKey(), dataCommand.getFlagsBitSet());
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }

      // Don't send a 2PC prepare at all if the cache is in degraded mode
      if (partitionHandlingManager.getAvailabilityMode() != AvailabilityMode.AVAILABLE &&
            !command.isOnePhaseCommit() && ctx.hasModifications()) {
         for (Object key : ctx.getAffectedKeys()) {
            partitionHandlingManager.checkWrite(key);
         }
      }
      return invokeNextThenAccept(ctx, command, postTxCommandCheck);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenAccept(ctx, command, postTxCommandCheck);
   }

   protected void postTxCommandCheck(InvocationContext rCtx, VisitableCommand rCommand, Object rv) {
      TxInvocationContext ctx = (TxInvocationContext) rCtx;
      if (partitionHandlingManager.getAvailabilityMode() != AvailabilityMode.AVAILABLE &&
            !partitionHandlingManager.isTransactionPartiallyCommitted(ctx.getGlobalTransaction()) &&
            ctx.hasModifications()) {
         for (Object key : ctx.getAffectedKeys()) {
            partitionHandlingManager.checkWrite(key);
         }
      }
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, handleGetAllCommandReturn);
   }

   private void handleGetAllCommandReturn(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable t) throws Throwable {
      GetAllCommand getAllCommand = (GetAllCommand) rCommand;
      if (t != null) {
         if (t instanceof RpcException && performPartitionCheck(rCtx, getAllCommand)) {
            // We must have received an AvailabilityException from one of the owners.
            // There is no way to verify the cause here, but there isn't any other way to get an invalid
            // get response.
            throw log.degradedModeKeysUnavailable(((GetAllCommand) rCommand).getKeys());
         }
      }

      if (!performPartitionCheck(rCtx, getAllCommand))
         return;

      // We do the availability check after the read, because the cache may have entered degraded mode
      // while we were reading from a remote node.
      for (Object key : getAllCommand.getKeys()) {
         partitionHandlingManager.checkRead(key, getAllCommand.getFlagsBitSet());
      }

      // TODO Dan: If we retry on CacheNotFoundResponse, we never have to deal with a smaller map here
      // Scattered cache throws AllOwnersLostException instead of returning map with only a subset of keys
      // because the owners might be unknown even if there's no data loss and then the command has to be retried.
      if (t == null && rv instanceof Map) {
         // rv could be UnsureResponse
         Map<Object, Object> result = ((Map<Object, Object>) rv);
         if (result.size() != getAllCommand.getKeys().size()) {
            Set<Object> missingKeys = new HashSet<>(getAllCommand.getKeys());
            missingKeys.removeAll(result.keySet());
            throw log.degradedModeKeysUnavailable(missingKeys);
         }
      }
   }
}
