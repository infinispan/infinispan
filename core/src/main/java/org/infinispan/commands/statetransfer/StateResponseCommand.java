package org.infinispan.commands.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.conflict.impl.StateReceiver;
import org.infinispan.factories.ComponentRegistry;
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
public class StateResponseCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   private static final Log log = LogFactory.getLog(StateResponseCommand.class);

   public static final byte COMMAND_ID = 20;

   /**
    * The topology id of the sender at send time.
    */
   private int topologyId;

   /**
    * A collections of state chunks to be transferred.
    */
   private Collection<StateChunk> stateChunks;

   /**
    * Whether the returned state should be applied to the underlying cache upon delivery
    */
   private boolean applyState;

   private StateResponseCommand() {
      super(null);  // for command id uniqueness test
   }

   public StateResponseCommand(ByteString cacheName) {
      super(cacheName);
   }

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
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(stateChunks, output);
      output.writeBoolean(applyState);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      stateChunks = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      applyState = input.readBoolean();
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
