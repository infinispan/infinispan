package org.infinispan.commands.statetransfer;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * Start state transfer.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class StateTransferStartCommand extends AbstractStateTransferCommand {

   public static final byte COMMAND_ID = 116;

   // For command id uniqueness test only
   public StateTransferStartCommand() {
      this(null);
   }

   public StateTransferStartCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public StateTransferStartCommand(ByteString cacheName, int topologyId, IntSet segments) {
      super(COMMAND_ID, cacheName, topologyId, segments);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      StateProvider stateProvider = registry.getStateTransferManager().getStateProvider();
      stateProvider.startOutboundTransfer(origin, topologyId, segments, true);
      return CompletableFutures.completedNull();
   }

   @Override
   public String toString() {
      return "StateTransferStartCommand{" +
            "topologyId=" + topologyId +
            ", segments=" + segments +
            ", cacheName=" + cacheName +
            '}';
   }
}
