package org.infinispan.commands.statetransfer;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.util.ByteString;

/**
 * Get transactions for the specified segments.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.STATE_TRANSFER_GET_TRANSACTIONS_COMMAND)
public class StateTransferGetTransactionsCommand extends AbstractStateTransferCommand {

   @ProtoFactory
   StateTransferGetTransactionsCommand(ByteString cacheName, int topologyId, WrappedMessage wrappedSegments) {
      this(cacheName, topologyId, WrappedMessages.<IntSet>unwrap(wrappedSegments));
   }

   public StateTransferGetTransactionsCommand(ByteString cacheName, int topologyId, IntSet segments) {
      super(cacheName, topologyId, segments);
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
