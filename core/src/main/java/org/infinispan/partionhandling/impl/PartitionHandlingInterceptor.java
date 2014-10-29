package org.infinispan.partionhandling.impl;

import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.read.EntryRetrievalCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.transport.Transport;

import java.util.Set;

public class PartitionHandlingInterceptor extends CommandInterceptor {

   PartitionHandlingManager partitionHandlingManager;
   private Transport transport;
   private ClusteringDependentLogic cdl;

   @Inject
   void init(PartitionHandlingManager partitionHandlingManager, Transport transport, ClusteringDependentLogic cdl) {
      this.partitionHandlingManager = partitionHandlingManager;
      this.transport = transport;
      this.cdl = cdl;
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
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkWrite(command.getKey());
      }
      return super.visitPutKeyValueCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkWrite(command.getKey());
      }
      return super.visitRemoveCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkWrite(command.getKey());
      }
      return super.visitReplaceCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         for (Object k : command.getAffectedKeys())
            partitionHandlingManager.checkWrite(k);
      }
      return super.visitPutMapCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkClear();
      }
      return super.visitClearCommand(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkWrite(command.getKey());
      }
      return super.visitApplyDeltaCommand(ctx, command);
   }

   @Override
   public Object visitEntryRetrievalCommand(InvocationContext ctx, EntryRetrievalCommand command) throws Throwable {
      if (partitionHandlingManager.getAvailabilityMode() != AvailabilityMode.AVAILABLE &&
            performPartitionCheck(ctx, command)) {
         throw getLog().partitionUnavailable();
      }
      return super.visitEntryRetrievalCommand(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
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
            if (!InfinispanCollections.containsAny(transport.getMembers(), cdl.getOwners(key))) {
               throw getLog().degradedModeKeyUnavailable(key);
            }
         }
      }

      // TODO We can still return a stale value if the other partition stayed active without us and we haven't entered degraded mode yet.
      return result;
   }
}
