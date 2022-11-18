package org.infinispan.commands.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseTopologyRpcCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.util.ByteString;

/**
 * Get the registered cluster listeners.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class StateTransferGetListenersCommand extends BaseTopologyRpcCommand implements TopologyAffectedCommand {

   public static final byte COMMAND_ID = 118;

   // For command id uniqueness test only
   public StateTransferGetListenersCommand() {
      this(null);
   }

   public StateTransferGetListenersCommand(ByteString cacheName) {
      super(cacheName, EnumUtil.EMPTY_BIT_SET);
   }

   public StateTransferGetListenersCommand(ByteString cacheName, int topologyId) {
      this(cacheName);
      this.topologyId = topologyId;
   }

   @Override
   public CompletionStage<Collection<ClusterListenerReplicateCallable<Object, Object>>> invokeAsync(ComponentRegistry registry) throws Throwable {
      StateProvider stateProvider = registry.getStateTransferManager().getStateProvider();
      Collection<ClusterListenerReplicateCallable<Object, Object>> listeners = stateProvider.getClusterListenersToInstall();
      return CompletableFuture.completedFuture(listeners);
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
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
   }

   @Override
   public String toString() {
      return "StateTransferGetListenersCommand{" +
            "topologyId=" + topologyId +
            ", cacheName=" + cacheName +
            '}';
   }
}
