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
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

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
      distributionManager = componentRegistry.getDistributionManager();
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
      if (distributionManager != null) {
         int currentTopologyId = distributionManager.getCacheTopology().getTopologyId();
         if (currentTopologyId != topologyId) {
            return CompletableFutures.completedExceptionFuture(OutdatedTopologyException.RETRY_NEXT_TOPOLOGY);
         }
      }
      return CompletableFuture.completedFuture(touched);
   }
}
