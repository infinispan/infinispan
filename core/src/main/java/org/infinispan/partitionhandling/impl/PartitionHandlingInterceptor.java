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
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.distribution.MissingOwnerException;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class PartitionHandlingInterceptor extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(PartitionHandlingInterceptor.class);
   
   private PartitionHandlingManager partitionHandlingManager;
   private Transport transport;
   private DistributionManager distributionManager;
   private CacheMode cacheMode;
   private StateTransferLock stateTransferLock;

   @Inject
   void init(PartitionHandlingManager partitionHandlingManager, Transport transport,
             DistributionManager distributionManager, Configuration configuration,
             StateTransferLock stateTransferLock) {
      this.partitionHandlingManager = partitionHandlingManager;
      this.transport = transport;
      this.distributionManager = distributionManager;
      this.cacheMode = configuration.clustering().cacheMode();
      this.stateTransferLock = stateTransferLock;
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
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         DataCommand dataCommand = (DataCommand) rCommand;
         if (throwable != null) {
            if (throwable instanceof RpcException && performPartitionCheck(rCtx, dataCommand)) {
               // We must have received an AvailabilityException from one of the owners.
               // There is no way to verify the cause here, but there isn't any other way to get an invalid
               // get response.
               throw log.degradedModeKeyUnavailable(dataCommand.getKey());
            } else if (throwable instanceof MissingOwnerException) {
               return handleMissingOwner(Collections.singleton(dataCommand.getKey()), (MissingOwnerException) throwable);
            } else {
               // If all owners left and we still haven't received the availability update yet,
               // we get TimeoutException from JGroupsTransport.checkRsps
               if (throwable instanceof TimeoutException && performPartitionCheck(rCtx, dataCommand)) {
                  // Unlike in PartitionHandlingManager.checkRead(), here we ignore the availability status
                  // and we only fail the operation if _all_ owners have left the cluster.
                  // TODO Move this to the availability strategy when implementing ISPN-4624
                  List<Address> owners = distributionManager.locate(dataCommand.getKey());
                  if (!InfinispanCollections.containsAny(transport.getMembers(), owners)) {
                     throw log.degradedModeKeyUnavailable(dataCommand.getKey());
                  }
               }
               throw throwable;
            }
         }

         postOperationPartitionCheck(rCtx, dataCommand, dataCommand.getKey());
         return null;
      });
   }

   private CompletableFuture<Object> handleMissingOwner(Collection keys, MissingOwnerException moe) throws InterruptedException {
      // In scattered cache mode it is possible that the CH does not contain owner of the key
      // and therefore throws MOE.
      // At this point we may be still available, so we have to wait until happens one of
      // a) we become degraded
      // b) topology change

      // do an early check for degraded mode
      if (partitionHandlingManager.getAvailabilityMode() == AvailabilityMode.DEGRADED_MODE) {
         throw log.degradedModeKeyUnavailable(keys);
      }
      int topologyId = moe.getTopologyId();
      log.tracef("Missing owner for %s in topology %d, will wait for new topology or becoming degraded",
         keys, topologyId);
      CompletableFuture<Void> topologyFuture = stateTransferLock.topologyFuture(topologyId + 1);
      if (topologyFuture == null) {
         // newer topology is already installed, retry with this one
         throw moe; // should be handled in the same way as OutdatedTopologyException
      }
      CompletableFuture<AvailabilityMode> degraded = partitionHandlingManager.degradedFuture();
      return CompletableFuture.anyOf(degraded, topologyFuture).thenApply(result -> {
         // upon topology change the result is null (void)
         if (result == AvailabilityMode.DEGRADED_MODE) {
            throw log.degradedModeKeysUnavailable(keys);
         } else {
            throw moe;
         }
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

   private void postOperationPartitionCheck(InvocationContext ctx, DataCommand command, Object key) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         // We do the availability check after the read, because the cache may have entered degraded mode
         // while we were reading from a remote node.
         partitionHandlingManager.checkRead(key);
      }
      // TODO We can still return a stale value if the other partition stayed active without us and we haven't entered degraded mode yet.
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
            } else if (throwable instanceof MissingOwnerException) {
               return handleMissingOwner(getAllCommand.getKeys(), (MissingOwnerException) throwable);
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
