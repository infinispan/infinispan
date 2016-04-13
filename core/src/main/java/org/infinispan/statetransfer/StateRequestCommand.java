package org.infinispan.statetransfer;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

/**
 * This command is used by a StateConsumer to request transactions and cache entries from a StateProvider.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateRequestCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   private static final Log log = LogFactory.getLog(StateRequestCommand.class);

   public enum Type {
      GET_TRANSACTIONS,
      GET_CACHE_LISTENERS,
      START_STATE_TRANSFER,
      CANCEL_STATE_TRANSFER;

      private static final Type[] CACHED_VALUES = values();
   }

   public static final byte COMMAND_ID = 15;

   private Type type = Type.CANCEL_STATE_TRANSFER; //default value for org.infinispan.remoting.AsynchronousInvocationTest

   private int topologyId;

   private Set<Integer> segments;

   private StateProvider stateProvider;

   private StateRequestCommand() {
      super(null);  // for command id uniqueness test
   }

   public StateRequestCommand(ByteString cacheName) {
      super(cacheName);
   }

   public StateRequestCommand(ByteString cacheName, Type type, Address origin, int topologyId, Set<Integer> segments) {
      super(cacheName);
      this.type = type;
      setOrigin(origin);
      this.topologyId = topologyId;
      this.segments = segments;
   }

   public void init(StateProvider stateProvider) {
      this.stateProvider = stateProvider;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         switch (type) {
            case GET_TRANSACTIONS:
               return stateProvider.getTransactionsForSegments(getOrigin(), topologyId, segments);

            case START_STATE_TRANSFER:
               stateProvider.startOutboundTransfer(getOrigin(), topologyId, segments);
               // return a non-null value to ensure it will reach back to originator wrapped in a SuccessfulResponse (a null would not be sent back)
               return true;

            case CANCEL_STATE_TRANSFER:
               stateProvider.cancelOutboundTransfer(getOrigin(), topologyId, segments);
               // originator does not care about the result, so we can return null
               return null;

            case GET_CACHE_LISTENERS:
               return stateProvider.getClusterListenersToInstall();
            default:
               throw new CacheException("Unknown state request command type: " + type);
         }
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   @Override
   public boolean isReturnValueExpected() {
      return type != Type.CANCEL_STATE_TRANSFER;
   }

   @Override
   public boolean canBlock() {
      // All state request commands need to wait for the proper topology
      return true;
   }

   public Type getType() {
      return type;
   }

   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public Set<Integer> getSegments() {
      return segments;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallEnum(type, output);
      switch (type) {
         case GET_TRANSACTIONS:
         case START_STATE_TRANSFER:
         case CANCEL_STATE_TRANSFER:
            output.writeObject(getOrigin());
            MarshallUtil.marshallCollection(segments, output);
            return;
         case GET_CACHE_LISTENERS:
            return;
         default:
            throw new IllegalStateException("Unknown state request command type: " + type);
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      type = MarshallUtil.unmarshallEnum(input, ordinal -> Type.CACHED_VALUES[ordinal]);
      switch (type) {
         case GET_TRANSACTIONS:
         case CANCEL_STATE_TRANSFER:
         case START_STATE_TRANSFER:
            setOrigin((Address) input.readObject());
            segments = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
         case GET_CACHE_LISTENERS:
            return;
         default:
            throw new IllegalStateException("Unknown state request command type: " + type);
      }
   }

   @Override
   public String toString() {
      return "StateRequestCommand{" +
            "cache=" + cacheName +
            ", origin=" + getOrigin() +
            ", type=" + type +
            ", topologyId=" + topologyId +
            ", segments=" + segments +
            '}';
   }
}
