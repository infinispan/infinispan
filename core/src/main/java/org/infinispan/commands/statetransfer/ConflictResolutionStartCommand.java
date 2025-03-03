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
 * Start conflict resolution.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CONFLICT_RESOLUTION_START_COMMAND)
public class ConflictResolutionStartCommand extends AbstractStateTransferCommand {

   @ProtoFactory
   ConflictResolutionStartCommand(ByteString cacheName, int topologyId, WrappedMessage wrappedSegments) {
      this(cacheName, topologyId, WrappedMessages.<IntSet>unwrap(wrappedSegments));
   }

   public ConflictResolutionStartCommand(ByteString cacheName, int topologyId, IntSet segments) {
      super(cacheName, topologyId, segments);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      StateProvider stateProvider = registry.getStateTransferManager().getStateProvider();
      stateProvider.startOutboundTransfer(origin, topologyId, segments, false);
      return CompletableFutures.completedNull();
   }

   @Override
   public String toString() {
      return "ConflictResolutionStartCommand{" +
            "topologyId=" + topologyId +
            ", segments=" + segments +
            ", cacheName=" + cacheName +
            '}';
   }
}
