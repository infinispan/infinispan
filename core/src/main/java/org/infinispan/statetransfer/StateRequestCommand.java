package org.infinispan.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scattered.BiasManager;
import org.infinispan.scattered.ScatteredStateProvider;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
      START_CONSISTENCY_CHECK,
      CANCEL_CONSISTENCY_CHECK,
      START_KEYS_TRANSFER,
      START_STATE_TRANSFER,
      CANCEL_STATE_TRANSFER,
      CONFIRM_REVOKED_SEGMENTS,
      ;

      private static final Type[] CACHED_VALUES = values();
   }

   public static final byte COMMAND_ID = 15;

   private Type type = Type.CANCEL_STATE_TRANSFER; //default value for org.infinispan.remoting.AsynchronousInvocationTest

   private int topologyId;

   private IntSet segments;

   private StateProvider stateProvider;
   private BiasManager biasManager;

   private StateRequestCommand() {
      super(null);  // for command id uniqueness test
   }

   public StateRequestCommand(ByteString cacheName) {
      super(cacheName);
   }

   public StateRequestCommand(ByteString cacheName, Type type, Address origin, int topologyId, IntSet segments) {
      super(cacheName);
      this.type = type;
      setOrigin(origin);
      this.topologyId = topologyId;
      this.segments = segments;
   }

   public void init(StateProvider stateProvider, BiasManager biasManager) {
      this.stateProvider = stateProvider;
      this.biasManager = biasManager;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         switch (type) {
            case GET_TRANSACTIONS:
               List<TransactionInfo> transactions =
                     stateProvider.getTransactionsForSegments(getOrigin(), topologyId, segments);
               return CompletableFuture.completedFuture(transactions);

            case START_CONSISTENCY_CHECK:
               stateProvider.startOutboundTransfer(getOrigin(), topologyId, segments, false);
               return CompletableFutures.completedNull();

            case START_KEYS_TRANSFER:
               ((ScatteredStateProvider) stateProvider).startKeysTransfer(segments, getOrigin());
               return CompletableFutures.completedNull();

            case START_STATE_TRANSFER:
               stateProvider.startOutboundTransfer(getOrigin(), topologyId, segments, true);
               return CompletableFutures.completedNull();

            case CANCEL_CONSISTENCY_CHECK:
            case CANCEL_STATE_TRANSFER:
               stateProvider.cancelOutboundTransfer(getOrigin(), topologyId, segments);
               return CompletableFutures.completedNull();

            case GET_CACHE_LISTENERS:
               Collection<ClusterListenerReplicateCallable<Object, Object>> listeners = stateProvider.getClusterListenersToInstall();
               return CompletableFuture.completedFuture(listeners);

            case CONFIRM_REVOKED_SEGMENTS:
               return ((ScatteredStateProvider) stateProvider).confirmRevokedSegments(topologyId)
                     .thenApply(nil -> {
                        if (biasManager != null) {
                           biasManager.revokeLocalBiasForSegments(segments);
                        }
                        return null;
                     });
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

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public IntSet getSegments() {
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
         case START_CONSISTENCY_CHECK:
         case CANCEL_CONSISTENCY_CHECK:
         case START_KEYS_TRANSFER:
         case START_STATE_TRANSFER:
         case GET_TRANSACTIONS:
         case CANCEL_STATE_TRANSFER:
            output.writeObject(getOrigin());
         case CONFIRM_REVOKED_SEGMENTS:
            output.writeObject(segments);
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
         case START_CONSISTENCY_CHECK:
         case CANCEL_CONSISTENCY_CHECK:
         case START_KEYS_TRANSFER:
         case START_STATE_TRANSFER:
         case GET_TRANSACTIONS:
         case CANCEL_STATE_TRANSFER:
            setOrigin((Address) input.readObject());
         case CONFIRM_REVOKED_SEGMENTS:
            segments = (IntSet) input.readObject();
            return;
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
