package org.infinispan.statetransfer;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;

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
    * This is injected on target node via init() method before the command is performed.
    */
   private StateConsumer stateConsumer;

   private StateResponseCommand() {
      super(null);  // for command id uniqueness test
   }

   public StateResponseCommand(ByteString cacheName) {
      super(cacheName);
   }

   public StateResponseCommand(ByteString cacheName, Address origin, int topologyId, Collection<StateChunk> stateChunks) {
      super(cacheName);
      setOrigin(origin);
      this.topologyId = topologyId;
      this.stateChunks = stateChunks;
   }

   public void init(StateConsumer stateConsumer) {
      this.stateConsumer = stateConsumer;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         stateConsumer.applyState(getOrigin(), topologyId, stateChunks);
         return null;
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
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      setOrigin((Address) input.readObject());
      topologyId = input.readInt();
      stateChunks = MarshallUtil.unmarshallCollection(input, ArrayList::new);
   }

   @Override
   public String toString() {
      return "StateResponseCommand{" +
            "cache=" + cacheName +
            ", stateChunks=" + stateChunks +
            ", origin=" + getOrigin() +
            ", topologyId=" + topologyId +
            '}';
   }
}
