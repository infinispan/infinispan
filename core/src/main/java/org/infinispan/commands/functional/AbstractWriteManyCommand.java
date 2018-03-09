package org.infinispan.commands.functional;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.impl.Params;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

public abstract class AbstractWriteManyCommand<K, V> implements WriteCommand, FunctionalCommand<K, V>, RemoteLockCommand {

   protected CommandInvocationId commandInvocationId;
   protected boolean isForwarded = false;
   protected int topologyId = -1;
   protected Params params;
   // TODO: this is used for the non-modifying read-write commands. Move required flags to Params
   // and make sure that ClusteringDependentLogic checks them.
   protected long flags;
   DataConversion keyDataConversion;
   DataConversion valueDataConversion;

   protected transient InvocationManager invocationManager;
   protected transient Set<Object> completedKeys;
   protected Map<Object, CommandInvocationId> lastInvocationIds;

   protected AbstractWriteManyCommand(CommandInvocationId commandInvocationId,
                                      Params params,
                                      DataConversion keyDataConversion,
                                      DataConversion valueDataConversion,
                                      InvocationManager invocationManager) {
      this.commandInvocationId = commandInvocationId;
      this.params = params;
      this.flags = params.toFlagsBitSet();
      this.invocationManager = invocationManager;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   protected AbstractWriteManyCommand(AbstractWriteManyCommand<K, V> command) {
      this.commandInvocationId = command.commandInvocationId;
      this.topologyId = command.topologyId;
      this.params = command.params;
      this.flags = command.flags;
      this.topologyId = command.topologyId;
      this.invocationManager = command.invocationManager;
      this.lastInvocationIds = command.lastInvocationIds;
   }

   protected AbstractWriteManyCommand() {
   }

   public void init(InvocationManager invocationManager, ComponentRegistry componentRegistry) {
      this.invocationManager = invocationManager;
      init(componentRegistry);
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
   public CommandInvocationId getCommandInvocationId() {
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

   @Override
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @Override
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   /**
    * The command has been already executed for this command, don't execute (e.g. persist into cache store) again
    * @param key
    * @param isCompleted
    */
   @Override
   public void setCompleted(Object key, boolean isCompleted) {
      if (isCompleted) {
         if (completedKeys == null) {
            completedKeys = new HashSet<>();
         }
         completedKeys.add(key);
      } else {
         if (completedKeys != null) {
            completedKeys.remove(key);
         }
      }
   }

   @Override
   public boolean isCompleted(Object key) {
      return completedKeys != null && completedKeys.contains(key);
   }

   @Override
   public Stream<?> completedKeys() {
      return completedKeys == null ? Stream.empty() : completedKeys.stream();
   }

   protected void recordInvocation(InvocationContext ctx, CacheEntry e, Object previousValue, Metadata previousMetadata) {
      if (invocationManager == null || (invocationManager.isSynchronous() && ctx.isOriginLocal()) || commandInvocationId == null) {
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
      e.setMetadata(builder.invocations(purged).invocation(commandInvocationId, previousValue, previousMetadata, now).build());
   }

   @Override
   public CommandInvocationId getLastInvocationId(Object key) {
      return lastInvocationIds == null ? null : lastInvocationIds.get(key);
   }

   @Override
   public void setLastInvocationId(Object key, CommandInvocationId id) {
      if (lastInvocationIds == null) {
         lastInvocationIds = new HashMap<>();
      }
      CommandInvocationId prev = lastInvocationIds.put(key, id);
      assert prev == null || prev.equals(id);
   }

   protected abstract void init(ComponentRegistry componentRegistry);

   public Map<Object, CommandInvocationId> getLastInvocationIds() {
      return lastInvocationIds;
   }

   public void setLastInvocationIds(Map<Object, CommandInvocationId> lastInvocationIds) {
      this.lastInvocationIds = lastInvocationIds;
   }
}
