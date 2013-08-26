package org.infinispan.statetransfer;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;

/**
 * This command is used by a StateConsumer to request transactions and cache entries from a StateProvider.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateRequestCommand extends BaseRpcCommand {

   private static final Log log = LogFactory.getLog(StateRequestCommand.class);

   public enum Type {
      GET_TRANSACTIONS,
      START_STATE_TRANSFER,
      CANCEL_STATE_TRANSFER
   }

   public static final byte COMMAND_ID = 15;

   private Type type = Type.CANCEL_STATE_TRANSFER; //default value for org.infinispan.remoting.AsynchronousInvocationTest

   private int topologyId;

   private Set<Integer> segments;

   private StateProvider stateProvider;

   private StateRequestCommand() {
      super(null);  // for command id uniqueness test
   }

   public StateRequestCommand(String cacheName) {
      super(cacheName);
   }

   public StateRequestCommand(String cacheName, Type type, Address origin, int topologyId, Set<Integer> segments) {
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
      return type == Type.GET_TRANSACTIONS || type == Type.START_STATE_TRANSFER;
   }

   public Type getType() {
      return type;
   }

   public int getTopologyId() {
      return topologyId;
   }

   public Set<Integer> getSegments() {
      return segments;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{(byte) type.ordinal(), getOrigin(), topologyId, segments};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      type = Type.values()[(Byte) parameters[i++]];
      setOrigin((Address) parameters[i++]);
      topologyId = (Integer) parameters[i++];
      segments = (Set<Integer>) parameters[i];
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
