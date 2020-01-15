package org.infinispan.expiration.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.time.TimeService;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * This command updates a cache entry's last access timestamp. If eviction is enabled, it will also update the recency information
 * <p>
 * This command returns a Boolean that is whether this command was able to touch the value or not.
 */
public class TouchCommand extends BaseRpcCommand implements InitializableCommand, TopologyAffectedCommand {
   public static final byte COMMAND_ID = 66;

   private Object key;
   private int segment;
   private int topologyId = -1;

   private InternalDataContainer internalDataContainer;
   private TimeService timeService;
   private DistributionManager distributionManager;

   // Only here for CommandIdUniquenessTest
   private TouchCommand() { super(null); }

   public TouchCommand(ByteString cacheName) {
      super(cacheName);
   }

   public TouchCommand(ByteString cacheName, Object key, int segment) {
      super(cacheName);
      this.key = key;
      this.segment = segment;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      UnsignedNumeric.writeUnsignedInt(output, segment);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      segment = UnsignedNumeric.readUnsignedInt(input);
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      internalDataContainer = componentRegistry.getInternalDataContainer().running();
      timeService = componentRegistry.getTimeService();
      // Invalidation cache doesn't set topology id - so we don't want to throw OTE in invokeAsync
      if (!componentRegistry.getConfiguration().clustering().cacheMode().isInvalidation()) {
         distributionManager = componentRegistry.getDistributionManager();
      }
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() {
      boolean touched = internalDataContainer.touch(segment, key, timeService.wallClockTime());
      // Hibernate currently disables clustered expiration manager, which means we can have a topology id of -1
      // when using a clustered cache mode
      if (distributionManager != null && topologyId != -1) {
         LocalizedCacheTopology lct = distributionManager.getCacheTopology();
         int currentTopologyId = lct.getTopologyId();
         if (currentTopologyId != topologyId) {
            return CompletableFutures.completedExceptionFuture(OutdatedTopologyException.RETRY_NEXT_TOPOLOGY);
         }
         DistributionInfo di = lct.getSegmentDistribution(segment);
         // If our node is a write owner but not read owner, that means we may not have the value yet - so we just
         // say we were touched anyways
         // TODO: There is a small window where the access time may not be updated which is described in https://issues.redhat.com/browse/ISPN-11144
         if (di.isWriteOwner() && !di.isReadOwner()) {
            return CompletableFuture.completedFuture(Boolean.TRUE);
         }
      }
      return CompletableFuture.completedFuture(touched);
   }
}
