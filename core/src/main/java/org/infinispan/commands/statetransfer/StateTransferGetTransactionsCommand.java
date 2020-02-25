package org.infinispan.commands.statetransfer;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.util.ByteString;

/**
 * Get transactions for the specified segments.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class StateTransferGetTransactionsCommand extends AbstractStateTransferCommand {

   public static final byte COMMAND_ID = 119;

   // For command id uniqueness test only
   public StateTransferGetTransactionsCommand() {
      this(null);
   }

   public StateTransferGetTransactionsCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public StateTransferGetTransactionsCommand(ByteString cacheName, int topologyId, IntSet segments) {
      super(COMMAND_ID, cacheName, topologyId, segments);
   }

   @Override
   public CompletionStage<List<TransactionInfo>> invokeAsync(ComponentRegistry registry) throws Throwable {
      StateProvider stateProvider = registry.getStateTransferManager().getStateProvider();
      return stateProvider.getTransactionsForSegments(origin, topologyId, segments);
   }

   @Override
   public String toString() {
      return "StateTransferGetTransactionsCommand{" +
            "topologyId=" + topologyId +
            ", segments=" + segments +
            ", cacheName=" + cacheName +
            '}';
   }
}
