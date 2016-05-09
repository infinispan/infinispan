package org.infinispan.partitionhandling.impl;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class PartitionHandlingInterceptor extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(PartitionHandlingInterceptor.class);
   
   PartitionHandlingManager partitionHandlingManager;
   private Transport transport;
   private DistributionManager distributionManager;

   @Inject
   void init(PartitionHandlingManager partitionHandlingManager, Transport transport, DistributionManager distributionManager) {
      this.partitionHandlingManager = partitionHandlingManager;
      this.transport = transport;
      this.distributionManager = distributionManager;
   }

   private boolean performPartitionCheck(InvocationContext ctx, LocalFlagAffectedCommand command) {
      // We always perform partition check if this is a remote command
      if (!ctx.isOriginLocal()) {
         return true;
      }
      Set<Flag> flags = command.getFlags();
      return flags == null || !flags.contains(Flag.CACHE_MODE_LOCAL);
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   protected CompletableFuture<Void> handleSingleWrite(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkWrite(command.getKey());
      }
      return handleDefault(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         for (Object k : command.getAffectedKeys())
            partitionHandlingManager.checkWrite(k);
      }
      return handleDefault(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkClear();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkBulkRead();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkBulkRead();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public final CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleDataReadCommand(ctx, command);
   }

   @Override
   public final CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return handleDataReadCommand(ctx, command);
   }

   private CompletableFuture<Void> handleDataReadCommand(InvocationContext ctx, DataCommand command) {
      Object key = command.getKey();
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            if (throwable instanceof RpcException && performPartitionCheck(rCtx, ((DataCommand) rCommand))) {
               // We must have received an AvailabilityException from one of the owners.
               // There is no way to verify the cause here, but there isn't any other way to get an invalid
               // get response.
               throw log.degradedModeKeyUnavailable(key);
            } else {
               throw throwable;
            }
         }

         postOperationPartitionCheck(rCtx, ((DataCommand) rCommand), key, rv);
         return null;
      });
   }

   @Override
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         return ctx.continueInvocation();
      }
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable == null) {
            postTxCommandCheck(((TxInvocationContext) rCtx));
         }
         return null;
      });
   }

   @Override
   public CompletableFuture<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         return ctx.continueInvocation();
      }
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable == null) {
            postTxCommandCheck(((TxInvocationContext) rCtx));
         }
         return null;
      });
   }

   protected void postTxCommandCheck(TxInvocationContext ctx) {
      if (ctx.hasModifications() && partitionHandlingManager.getAvailabilityMode() != AvailabilityMode.AVAILABLE && !partitionHandlingManager.isTransactionPartiallyCommitted(ctx.getGlobalTransaction())) {
         for (Object key : ctx.getAffectedKeys()) {
            partitionHandlingManager.checkWrite(key);
         }
      }
   }

   private Object postOperationPartitionCheck(InvocationContext ctx, DataCommand command, Object key, Object result) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         // We do the availability check after the read, because the cache may have entered degraded mode
         // while we were reading from a remote node.
         partitionHandlingManager.checkRead(key);

         // If all owners left and we still haven't received the availability update yet, we could return
         // an incorrect null value. So we need a special check for null results.
         if (result == null) {
            // Unlike in PartitionHandlingManager.checkRead(), here we ignore the availability status
            // and we only fail the operation if _all_ owners have left the cluster.
            // TODO Move this to the availability strategy when implementing ISPN-4624
            if (!InfinispanCollections.containsAny(transport.getMembers(), distributionManager.locate(key))) {
               throw log.degradedModeKeyUnavailable(key);
            }
         }
      }
      // TODO We can still return a stale value if the other partition stayed active without us and we haven't entered degraded mode yet.
      return result;
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         GetAllCommand getAllCommand = (GetAllCommand) rCommand;
         if (throwable != null) {
            if (throwable instanceof RpcException && performPartitionCheck(rCtx, getAllCommand)) {
               // We must have received an AvailabilityException from one of the owners.
               // There is no way to verify the cause here, but there isn't any other way to get an invalid
               // get response.
               throw log.degradedModeKeysUnavailable(((GetAllCommand) rCommand).getKeys());
            } else {
               throw throwable;
            }
         }

         if (performPartitionCheck(rCtx, getAllCommand)) {
            // We do the availability check after the read, because the cache may have entered degraded mode
            // while we were reading from a remote node.
            for (Object key : getAllCommand.getKeys()) {
               partitionHandlingManager.checkRead(key);
            }

            Map<Object, Object> result = ((Map<Object, Object>) rv);
            // If all owners left and we still haven't received the availability update yet, we could return
            // an incorrect value. So we need a special check for missing results.
            if (result.size() != getAllCommand.getKeys().size()) {
               // Unlike in PartitionHandlingManager.checkRead(), here we ignore the availability status
               // and we only fail the operation if _all_ owners have left the cluster.
               // TODO Move this to the availability strategy when implementing ISPN-4624
               Set<Object> missingKeys = new HashSet<>(getAllCommand.getKeys());
               missingKeys.removeAll(result.keySet());
               for (Iterator<Object> it = missingKeys.iterator(); it.hasNext(); ) {
                  Object key = it.next();
                  if (InfinispanCollections.containsAny(transport.getMembers(), distributionManager.locate(key))) {
                     it.remove();
                  }
               }
               if (!missingKeys.isEmpty()) {
                  throw log.degradedModeKeysUnavailable(missingKeys);
               }
            }
         }

         // TODO We can still return a stale value if the other partition stayed active without us and we haven't entered degraded mode yet.
         return null;
      });
   }
}
