package org.infinispan.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.conflict.impl.StateReceiver;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This command is used by a StateProvider to push cache entries to a StateConsumer.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateResponseCommand extends BaseRpcCommand {

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

   /**
    * These objects are injected on target node via init() method before the command is performed.
    */
   private StateConsumer stateConsumer;
   private StateReceiver stateReceiver;

   private StateResponseCommand() {
      super(null);  // for command id uniqueness test
   }

   public StateResponseCommand(ByteString cacheName) {
      super(cacheName);
   }

   public StateResponseCommand(ByteString cacheName, Address origin, int topologyId, Collection<StateChunk> stateChunks) {
      this(cacheName, origin, topologyId, stateChunks, true);
   }

   public StateResponseCommand(ByteString cacheName, Address origin, int topologyId, Collection<StateChunk> stateChunks,
                               boolean applyState) {
      super(cacheName);
      setOrigin(origin);
      this.topologyId = topologyId;
      this.stateChunks = stateChunks;
      this.applyState = applyState;
   }

   public void init(StateConsumer stateConsumer, StateReceiver stateReceiver) {
      this.stateConsumer = stateConsumer;
      this.stateReceiver = stateReceiver;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         if (applyState) {
            stateConsumer.applyState(getOrigin(), topologyId, stateChunks);
         } else {
            stateReceiver.receiveState(getOrigin(), topologyId, stateChunks);
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
   public boolean canBlock() {
      return true;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(getOrigin());
      output.writeInt(topologyId);
      MarshallUtil.marshallCollection(stateChunks, output);
      output.writeBoolean(applyState);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      setOrigin((Address) input.readObject());
      topologyId = input.readInt();
      stateChunks = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      applyState = input.readBoolean();
   }

   @Override
   public String toString() {
      return "StateResponseCommand{" +
            "cache=" + cacheName +
            ", stateChunks=" + stateChunks +
            ", origin=" + getOrigin() +
            ", topologyId=" + topologyId +
            ", applyState=" + applyState +
            '}';
   }
}
