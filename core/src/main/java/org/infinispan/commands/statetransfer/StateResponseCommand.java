package org.infinispan.commands.statetransfer;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.conflict.impl.StateReceiver;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This command is used by a StateProvider to push cache entries to a StateConsumer.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@ProtoTypeId(ProtoStreamTypeIds.STATE_RESPONSE_COMMAND)
public class StateResponseCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   private static final Log log = LogFactory.getLog(StateResponseCommand.class);

   public static final byte COMMAND_ID = 20;

   /**
    * The topology id of the sender at send time.
    */
   @ProtoField(2)
   int topologyId;

   /**
    * A collections of state chunks to be transferred.
    */
   @ProtoField(3)
   Collection<StateChunk> stateChunks;

   /**
    * Whether the returned state should be applied to the underlying cache upon delivery
    */
   @ProtoField(4)
   boolean applyState;

   @ProtoFactory
   public StateResponseCommand(ByteString cacheName, int topologyId, Collection<StateChunk> stateChunks,
                               boolean applyState) {
      super(cacheName);
      this.topologyId = topologyId;
      this.stateChunks = stateChunks;
      this.applyState = applyState;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         if (applyState) {
            StateConsumer stateConsumer = componentRegistry.getStateTransferManager().getStateConsumer();
            return stateConsumer.applyState(origin, topologyId, stateChunks);
         } else {
            StateReceiver stateReceiver = componentRegistry.getConflictManager().running().getStateReceiver();
            stateReceiver.receiveState(origin, topologyId, stateChunks);
         }
         return CompletableFutures.completedNull();
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
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

   public Collection<StateChunk> getStateChunks() {
      return stateChunks;
   }

   @Override
   public String toString() {
      return "StateResponseCommand{" +
            "cache=" + cacheName +
            ", stateChunks=" + stateChunks +
            ", origin=" + origin +
            ", topologyId=" + topologyId +
            ", applyState=" + applyState +
            '}';
   }
}
