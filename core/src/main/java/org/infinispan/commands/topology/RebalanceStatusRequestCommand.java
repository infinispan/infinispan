package org.infinispan.commands.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.topology.RebalancingStatus;

/**
 * Query the rebalancing status.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class RebalanceStatusRequestCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 90;

   private String cacheName;

   // For CommandIdUniquenessTest only
   public RebalanceStatusRequestCommand() {
      super(COMMAND_ID);
   }

   public RebalanceStatusRequestCommand(String cacheName) {
      super(COMMAND_ID);
      this.cacheName = cacheName;
   }

   @Override
   public CompletionStage<RebalancingStatus> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      RebalancingStatus status;
      if (cacheName == null) {
         status = gcr.getClusterTopologyManager().isRebalancingEnabled() ? RebalancingStatus.PENDING : RebalancingStatus.SUSPENDED;
      } else {
         status = gcr.getClusterTopologyManager().getRebalancingStatus(cacheName);
      }
      return CompletableFuture.completedFuture(status);
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(cacheName, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cacheName = MarshallUtil.unmarshallString(input);
   }

   @Override
   public String toString() {
      return "RebalanceStatusCommand{" +
            "cacheName='" + cacheName + '\'' +
            '}';
   }
}
