package org.infinispan.commands.statetransfer;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.scattered.ScatteredStateProvider;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * Start transferring keys and remote metadata for the given segments.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class ScatteredStateGetKeysCommand extends AbstractStateTransferCommand {

   public static final byte COMMAND_ID = 114;

   // For command id uniqueness test only
   public ScatteredStateGetKeysCommand() {
      this(null);
   }

   public ScatteredStateGetKeysCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public ScatteredStateGetKeysCommand(ByteString cacheName, int topologyId, IntSet segments) {
      super(COMMAND_ID, cacheName, topologyId, segments);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      ScatteredStateProvider stateProvider = (ScatteredStateProvider) registry.getStateTransferManager().getStateProvider();
      stateProvider.startKeysTransfer(segments, origin);
      return CompletableFutures.completedNull();
   }

   @Override
   public String toString() {
      return "ScatteredStateGetKeysCommand{" +
            "topologyId=" + topologyId +
            ", segments=" + segments +
            ", cacheName=" + cacheName +
            '}';
   }
}
