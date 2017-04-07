package org.infinispan.commands.functional;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.functional.impl.Params;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

public abstract class AbstractWriteManyCommand<K, V> implements WriteCommand, FunctionalCommand<K, V>, RemoteLockCommand {

   CommandInvocationId commandInvocationId;
   boolean isForwarded = false;
   int topologyId = -1;
   Params params;
   // TODO: this is used for the non-modifying read-write commands. Move required flags to Params
   // and make sure that ClusteringDependentLogic checks them.
   long flags;
   transient Set<Object> completedKeys;
   transient boolean authoritative;

   protected AbstractWriteManyCommand(CommandInvocationId commandInvocationId) {
      this.commandInvocationId = commandInvocationId;
   }

   protected AbstractWriteManyCommand() {
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

   protected void recordInvocation(CacheEntry e, Object result) {
      if (commandInvocationId == null) {
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
         builder = builder.maxIdle(-1).lifespan(-1).version(null);
      }
      e.setMetadata(builder.invocation(commandInvocationId, result, authoritative,
            e.isCreated(), !e.isCreated() && !e.isRemoved(), e.isRemoved(), 0).build());
   }
}
