package org.infinispan.commands.statetransfer;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * Cancel state transfer.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class StateTransferCancelCommand extends AbstractStateTransferCommand {

   public static final byte COMMAND_ID = 117;

   public StateTransferCancelCommand() {
      this(null);
   }

   public StateTransferCancelCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public StateTransferCancelCommand(ByteString cacheName, int topologyId, IntSet segments) {
      super(COMMAND_ID, cacheName, topologyId, segments);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      StateProvider stateProvider = registry.getStateTransferManager().getStateProvider();
      stateProvider.cancelOutboundTransfer(origin, topologyId, segments);
      return CompletableFutures.completedNull();
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public String toString() {
      return "StateTransferCancelCommand{" +
            "topologyId=" + topologyId +
            ", segments=" + segments +
            ", cacheName=" + cacheName +
            '}';
   }
}
