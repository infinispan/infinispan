package org.infinispan.commands.statetransfer;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.util.ByteString;

/**
 * Cancel state transfer.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.STATE_TRANSFER_CANCEL_COMMAND)
public class StateTransferCancelCommand extends AbstractStateTransferCommand {

   @ProtoFactory
   StateTransferCancelCommand(ByteString cacheName, int topologyId, WrappedMessage wrappedSegments) {
      this(cacheName, topologyId, WrappedMessages.<IntSet>unwrap(wrappedSegments));
   }

   public StateTransferCancelCommand(ByteString cacheName, int topologyId, IntSet segments) {
      super(cacheName, topologyId, segments);
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
