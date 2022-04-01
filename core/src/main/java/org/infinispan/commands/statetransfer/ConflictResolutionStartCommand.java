package org.infinispan.commands.statetransfer;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * Start conflict resolution.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class ConflictResolutionStartCommand extends AbstractStateTransferCommand {

   public static final byte COMMAND_ID = 112;

   // For command id uniqueness test only
   public ConflictResolutionStartCommand() {
      this(null);
   }

   public ConflictResolutionStartCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public ConflictResolutionStartCommand(ByteString cacheName, int topologyId, IntSet segments) {
      super(COMMAND_ID, cacheName, topologyId, segments);
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
