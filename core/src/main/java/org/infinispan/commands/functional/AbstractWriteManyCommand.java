package org.infinispan.commands.functional;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.functional.impl.Params;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

public abstract class AbstractWriteManyCommand<K, V> implements WriteCommand, FunctionalCommand<K, V>, RemoteLockCommand {

   CommandInvocationId commandInvocationId;
   boolean isForwarded = false;
   boolean synchronous;
   int topologyId = -1;
   Params params;
   // TODO: this is used for the non-modifying read-write commands. Move required flags to Params
   // and make sure that ClusteringDependentLogic checks them.
   long flags;
   transient InvocationManager invocationManager;
   transient Set<Object> completedKeys;
   transient boolean authoritative;

   protected AbstractWriteManyCommand(CommandInvocationId commandInvocationId, InvocationManager invocationManager, boolean synchronous) {
      this.commandInvocationId = commandInvocationId;
      this.invocationManager = invocationManager;
      this.synchronous = synchronous;
   }

   protected AbstractWriteManyCommand(AbstractWriteManyCommand<K, V> command) {
      this.commandInvocationId = command.commandInvocationId;
      this.params = command.params;
      this.flags = command.flags;
      this.topologyId = command.topologyId;
      this.invocationManager = command.invocationManager;
      this.synchronous = command.synchronous;
   }

   protected AbstractWriteManyCommand() {
   }

   public void init(InvocationManager invocationManager, boolean synchronous) {
      this.invocationManager = invocationManager;
      this.synchronous = synchronous;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public boolean isForwarded() {
      return isForwarded;
   }

   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public void fail() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }

   @Override
   public void setAuthoritative(boolean authoritative) {
      this.authoritative = authoritative;
   }

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      this.flags = bitSet;
   }

   @Override
   public Params getParams() {
      return params;
   }

   public void setParams(Params params) {
      this.params = params;
   }

   @Override
   public Object getKeyLockOwner() {
      return commandInvocationId;
   }

   @Override
   public boolean hasZeroLockAcquisition() {
      return hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public boolean hasSkipLocking() {
      return hasAnyFlag(FlagBitSets.SKIP_LOCKING);
   }

   /**
    * The command has been already executed for this command, don't execute (e.g. persist into cache store) again
    * @param key
    */
   @Override
   public void setCompleted(Object key) {
      if (completedKeys == null) {
         completedKeys = new HashSet<>();
      }
      completedKeys.add(key);
   }

   @Override
   public boolean isCompleted(Object key) {
      return completedKeys != null && completedKeys.contains(key);
   }

   protected void recordInvocation(InvocationContext ctx, CacheEntry e, Object result) {
      if ((synchronous && ctx.isOriginLocal()) || commandInvocationId == null || invocationManager == null) {
         return;
      }
      Metadata metadata = e.getMetadata();
      Metadata.Builder builder;
      if (metadata == null) {
         builder = new EmbeddedMetadata.Builder();
      } else {
         builder = metadata.builder();
      }
      if (e.isRemoved()) {
         builder = builder.maxIdle(-1).lifespan(invocationManager.invocationTimeout()).version(null);
      }
      long now = invocationManager.wallClockTime();
      InvocationRecord purged = InvocationRecord.purgeExpired(builder.invocations(), now - invocationManager.invocationTimeout());
      e.setMetadata(builder.invocations(purged).invocation(commandInvocationId, isReturnValueExpected() ? result : null, authoritative,
            e.isCreated(), !e.isCreated() && !e.isRemoved(), e.isRemoved(), now).build());
   }
}
