package org.infinispan.commands.statetransfer;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.util.ByteString;

/**
 * Get the registered cluster listeners.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.STATE_TRANSFER_GET_LISTENERS_COMMAND)
public class StateTransferGetListenersCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   public static final byte COMMAND_ID = 118;

   @ProtoField(number = 2, defaultValue = "-1")
   int topologyId;

   @ProtoFactory
   public StateTransferGetListenersCommand(ByteString cacheName, int topologyId) {
      super(cacheName);
      this.topologyId = topologyId;
   }

   @Override
   public CompletionStage<Collection<ClusterListenerReplicateCallable<Object, Object>>> invokeAsync(ComponentRegistry registry) throws Throwable {
      StateProvider stateProvider = registry.getStateTransferManager().getStateProvider();
      Collection<ClusterListenerReplicateCallable<Object, Object>> listeners = stateProvider.getClusterListenersToInstall();
      return CompletableFuture.completedFuture(listeners);
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
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      return "StateTransferGetListenersCommand{" +
            "topologyId=" + topologyId +
            ", cacheName=" + cacheName +
            '}';
   }
}
