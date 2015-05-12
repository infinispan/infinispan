package org.infinispan.partitionhandling.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.EntryRetrievalCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.GetAllCommand;
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
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.transport.Transport;

public class PartitionHandlingInterceptor extends CommandInterceptor {
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
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
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
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public Object visitEntryRetrievalCommand(InvocationContext ctx, EntryRetrievalCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkBulkRead();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      Object result;
      try {
         result = super.visitGetKeyValueCommand(ctx, command);
      } catch (RpcException e) {
         if (performPartitionCheck(ctx, command)) {
            // We must have received an AvailabilityException from one of the owners.
            // There is no way to verify the cause here, but there isn't any other way to get an invalid get response.
            throw getLog().degradedModeKeyUnavailable(key);
         } else {
            throw e;
         }
      }
      postOperationPartitionCheck(ctx, command, key, result);
      return result;
   }

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      Object key = command.getKey();
      Object result;
      try {
         result = super.visitGetCacheEntryCommand(ctx, command);
      } catch (RpcException e) {
         if (performPartitionCheck(ctx, command)) {
            // We must have received an AvailabilityException from one of the owners.
            // There is no way to verify the cause here, but there isn't any other way to get an invalid get response.
            throw getLog().degradedModeKeyUnavailable(key);
         } else {
            throw e;
         }
      }
      postOperationPartitionCheck(ctx, command, key, result);
      return result;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object result = super.visitPrepareCommand(ctx, command);
      if (ctx.isOriginLocal()) {
         postTxCommandCheck(ctx);
      }
      return result;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      Object result = super.visitCommitCommand(ctx, command);
      if (ctx.isOriginLocal()) {
         postTxCommandCheck(ctx);
      }
      return result;
   }

   protected void postTxCommandCheck(TxInvocationContext ctx) {
      if (ctx.hasModifications() && partitionHandlingManager.getAvailabilityMode() != AvailabilityMode.AVAILABLE && !partitionHandlingManager.isTransactionPartiallyCommitted(ctx.getGlobalTransaction())) {
         for (Object key : ctx.getAffectedKeys()) {
            partitionHandlingManager.checkWrite(key);
         }
      }
   }

   private Object postOperationPartitionCheck(InvocationContext ctx, AbstractDataCommand command, Object key, Object result) throws Throwable {
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
               throw getLog().degradedModeKeyUnavailable(key);
            }
         }
      }
      // TODO We can still return a stale value if the other partition stayed active without us and we haven't entered degraded mode yet.
      return result;
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      Map<Object, Object> result;
      try {
         result = (Map<Object, Object>) super.visitGetAllCommand(ctx, command);
      } catch (RpcException e) {
         if (performPartitionCheck(ctx, command)) {
            // We must have received an AvailabilityException from one of the owners.
            // There is no way to verify the cause here, but there isn't any other way to get an invalid get response.
            throw getLog().degradedModeKeysUnavailable(command.getKeys());
         } else {
            throw e;
         }
      }

      if (performPartitionCheck(ctx, command)) {
         // We do the availability check after the read, because the cache may have entered degraded mode
         // while we were reading from a remote node.
         for (Object key : command.getKeys()) {
            partitionHandlingManager.checkRead(key);
         }

         // If all owners left and we still haven't received the availability update yet, we could return
         // an incorrect value. So we need a special check for missing results.
         if (result.size() != command.getKeys().size()) {
            // Unlike in PartitionHandlingManager.checkRead(), here we ignore the availability status
            // and we only fail the operation if _all_ owners have left the cluster.
            // TODO Move this to the availability strategy when implementing ISPN-4624
            Set<Object> missingKeys = new HashSet<>(command.getKeys());
            missingKeys.removeAll(result.keySet());
            for (Iterator<Object> it = missingKeys.iterator(); it.hasNext();) {
               Object key = it.next();
               if (InfinispanCollections.containsAny(transport.getMembers(), distributionManager.locate(key))) {
                  it.remove();
               }
            }
            if (!missingKeys.isEmpty()) {
               throw getLog().degradedModeKeysUnavailable(missingKeys);
            }
         }
      }

      // TODO We can still return a stale value if the other partition stayed active without us and we haven't entered degraded mode yet.
      return result;
   }
}
