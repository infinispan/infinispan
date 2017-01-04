package org.infinispan.partitionhandling.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
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
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.function.TetraConsumer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PartitionHandlingInterceptor extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(PartitionHandlingInterceptor.class);

   private PartitionHandlingManager partitionHandlingManager;
   private Transport transport;
   private StateTransferManager stateTransferManager;

   private final TetraConsumer<InvocationContext, DataCommand, Object, Throwable>
         afterDataReadCommand = this::afterDataReadCommand;
   private final BiConsumer<TxInvocationContext, Object> afterTxCommand = this::afterTxCommand;
   private final TetraConsumer<InvocationContext, GetAllCommand, Object, Throwable>
         afterGetAllCommand = this::afterGetAllCommand;

   @Inject
   void init(PartitionHandlingManager partitionHandlingManager, Transport transport,
             StateTransferManager stateTransferManager) {
      this.partitionHandlingManager = partitionHandlingManager;
      this.transport = transport;
      this.stateTransferManager = stateTransferManager;
   }

   private boolean performPartitionCheck(InvocationContext ctx, FlagAffectedCommand command) {
      // We always perform partition check if this is a remote command
      if (!ctx.isOriginLocal()) {
         return true;
      }
      return !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL);
   }

   @Override
   public InvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public InvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public InvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   protected InvocationStage handleSingleWrite(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkWrite(command.getKey());
      }
      return handleDefault(ctx, command);
   }

   @Override
   public InvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         for (Object k : command.getAffectedKeys())
            partitionHandlingManager.checkWrite(k);
      }
      return handleDefault(ctx, command);
   }

   @Override
   public InvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkClear();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public InvocationStage visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command)
         throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public InvocationStage visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkBulkRead();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public InvocationStage visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkBulkRead();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public final InvocationStage visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      return handleDataReadCommand(ctx, command);
   }

   @Override
   public final InvocationStage visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return handleDataReadCommand(ctx, command);
   }

   private InvocationStage handleDataReadCommand(InvocationContext ctx, DataCommand command) {
      return invokeNext(ctx, command).whenComplete(ctx, command, afterDataReadCommand);
   }

   private void afterDataReadCommand(InvocationContext ctx, DataCommand command, Object ignored, Throwable t) {
      if (t != null) {
         if (t instanceof RpcException && performPartitionCheck(ctx, command)) {
            // We must have received an AvailabilityException from one of the owners.
            // There is no way to verify the cause here, but there isn't any other way to get an invalid
            // get response.
            throw log.degradedModeKeyUnavailable(command.getKey());
         } else {
            // If all owners left and we still haven't received the availability update yet,
            // we get OutdatedTopologyException from BaseDistributionInterceptor.retrieveFromProperSource
            if (t instanceof OutdatedTopologyException && performPartitionCheck(ctx, command)) {
               // Unlike in PartitionHandlingManager.checkRead(), here we ignore the availability status
               // and we only fail the operation if _all_ owners have left the cluster.
               // TODO Move this to the availability strategy when implementing ISPN-4624
               CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
               if (cacheTopology == null || cacheTopology.getTopologyId() != command.getTopologyId()) {
                  // just rethrow the exception
                  rethrowAsCompletedException(t);
               }
               List<Address> owners = cacheTopology.getReadConsistentHash()
                                                   .locateOwners(command.getKey());
               if (!InfinispanCollections.containsAny(transport.getMembers(), owners)) {
                  throw log.degradedModeKeyUnavailable(command.getKey());
               }
            }
            rethrowAsCompletedException(t);
         }
      }

      postOperationPartitionCheck(ctx, command, command.getKey());
   }

   @Override
   public InvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command)
            .thenAccept(ctx, afterTxCommand);
   }

   @Override
   public InvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command)
            .thenAccept(ctx, afterTxCommand);
   }

   protected void afterTxCommand(TxInvocationContext ctx, Object ignored) {
      if (ctx.hasModifications() && partitionHandlingManager.getAvailabilityMode() != AvailabilityMode.AVAILABLE && !partitionHandlingManager.isTransactionPartiallyCommitted(ctx.getGlobalTransaction())) {
         for (Object key : ctx.getAffectedKeys()) {
            partitionHandlingManager.checkWrite(key);
         }
      }
   }

   private void postOperationPartitionCheck(InvocationContext ctx, DataCommand command, Object key) {
      if (performPartitionCheck(ctx, command)) {
         // We do the availability check after the read, because the cache may have entered degraded mode
         // while we were reading from a remote node.
         partitionHandlingManager.checkRead(key);
      }
      // TODO We can still return a stale value if the other partition stayed active without us and we haven't entered degraded mode yet.
   }

   @Override
   public InvocationStage visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return invokeNext(ctx, command).whenComplete(ctx, command, afterGetAllCommand);
   }

   private void afterGetAllCommand(InvocationContext rctx, GetAllCommand command, Object rv,
                                   Throwable t) {
      if (t != null) {
         if (t instanceof RpcException && performPartitionCheck(rctx, command)) {
            // We must have received an AvailabilityException from one of the owners.
            // There is no way to verify the cause here, but there isn't any other way to get an invalid
            // get response.
            throw log.degradedModeKeysUnavailable(command.getKeys());
         } else {
            rethrowAsCompletedException(t);
         }
      }

      if (performPartitionCheck(rctx, command)) {
         // We do the availability check after the read, because the cache may have entered degraded mode
         // while we were reading from a remote node.
         for (Object key : command.getKeys()) {
            partitionHandlingManager.checkRead(key);
         }

         Map<Object, Object> result = (Map<Object, Object>) rv;
         // If all owners left and we still haven't received the availability update yet, we could return
         // an incorrect value. So we need a special check for missing results.
         if (result.size() != command.getKeys()
                                     .size()) {
            // Unlike in PartitionHandlingManager.checkRead(), here we ignore the availability status
            // and we only fail the operation if _all_ owners have left the cluster.
            // TODO Move this to the availability strategy when implementing ISPN-4624
            Set<Object> missingKeys = new HashSet<>(command.getKeys());
            missingKeys.removeAll(result.keySet());
            missingKeys.removeIf(key -> InfinispanCollections.containsAny(transport.getMembers(),
                                                                          stateTransferManager.getCacheTopology()
                                                                                              .getReadConsistentHash()
                                                                                              .locateOwners(key)));
            if (!missingKeys.isEmpty()) {
               throw log.degradedModeKeysUnavailable(missingKeys);
            }
         }
      }

      // TODO We can still return a stale value if the other partition stayed active without us and we haven't entered degraded mode yet.
   }
}
