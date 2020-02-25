package org.infinispan.commands.statetransfer;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.scattered.BiasManager;
import org.infinispan.scattered.ScatteredStateProvider;
import org.infinispan.util.ByteString;

/**
 * Invoke {@link ScatteredStateProvider#confirmRevokedSegments(int)}.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class ScatteredStateConfirmRevokedCommand extends AbstractStateTransferCommand {

   public static final byte COMMAND_ID = 115;

   // For command id uniqueness test only
   public ScatteredStateConfirmRevokedCommand() {
      this(null);
   }

   public ScatteredStateConfirmRevokedCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public ScatteredStateConfirmRevokedCommand(ByteString cacheName, int topologyId, IntSet segments) {
      super(COMMAND_ID, cacheName, topologyId, segments);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      ScatteredStateProvider stateProvider = (ScatteredStateProvider) registry.getStateTransferManager().getStateProvider();
      BiasManager biasManager = registry.getBiasManager().running();
      return stateProvider.confirmRevokedSegments(topologyId)
            .thenApply(nil -> {
               if (biasManager != null) {
                  biasManager.revokeLocalBiasForSegments(segments);
               }
               return null;
            });
   }

   @Override
   public String toString() {
      return "ScatteredStateConfirmRevokedCommand{" +
            "topologyId=" + topologyId +
            ", segments=" + segments +
            ", cacheName=" + cacheName +
            '}';
   }
}
