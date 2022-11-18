package org.infinispan.commands.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseTopologyRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
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
public class StateResponseCommand extends BaseTopologyRpcCommand implements TopologyAffectedCommand {

   private static final Log log = LogFactory.getLog(StateResponseCommand.class);

   public static final byte COMMAND_ID = 20;

   /**
    * A collections of state chunks to be transferred.
    */
   private Collection<StateChunk> stateChunks;

   /**
    * Whether the returned state should be applied to the underlying cache upon delivery
    */
   private boolean applyState;

   /**
    * Traditional state transfer is pull based (node sends {@link org.infinispan.commands.statetransfer.StateTransferStartCommand}
    * and expects StateResponseCommand). This flags unsolicited StateResponseCommand that should be applied anyway. Used
    * by scattered cache.
    */
   private boolean pushTransfer;

   private StateResponseCommand() {
      this(null);  // for command id uniqueness test
   }

   public StateResponseCommand(ByteString cacheName) {
      super(cacheName, EnumUtil.EMPTY_BIT_SET);
   }

   public StateResponseCommand(ByteString cacheName, int topologyId, Collection<StateChunk> stateChunks,
                               boolean applyState, boolean pushTransfer) {
      this(cacheName);
      this.topologyId = topologyId;
      this.stateChunks = stateChunks;
      this.applyState = applyState;
      this.pushTransfer = pushTransfer;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         if (applyState) {
            StateConsumer stateConsumer = componentRegistry.getStateTransferManager().getStateConsumer();
            return stateConsumer.applyState(origin, topologyId, pushTransfer, stateChunks);
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
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Collection<StateChunk> getStateChunks() {
      return stateChunks;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeBoolean(pushTransfer);
      MarshallUtil.marshallCollection(stateChunks, output);
      output.writeBoolean(applyState);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      pushTransfer = input.readBoolean();
      stateChunks = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      applyState = input.readBoolean();
   }

   @Override
   public String toString() {
      return "StateResponseCommand{" +
            "cache=" + cacheName +
            ", pushTransfer=" + pushTransfer +
            ", stateChunks=" + stateChunks +
            ", origin=" + origin +
            ", topologyId=" + topologyId +
            ", applyState=" + applyState +
            '}';
   }
}
