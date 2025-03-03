package org.infinispan.commands.topology;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.topology.RebalancingStatus;

/**
 * Query the rebalancing status.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REBALANCE_STATUS_REQUEST_COMMAND)
public class RebalanceStatusRequestCommand extends AbstractCacheControlCommand {

   @ProtoField(1)
   final String cacheName;

   public RebalanceStatusRequestCommand() {
      this(null);
   }

   @ProtoFactory
   public RebalanceStatusRequestCommand(String cacheName) {
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

   public String getCacheName() {
      return cacheName;
   }

   @Override
   public String toString() {
      return "RebalanceStatusCommand{" +
            "cacheName='" + cacheName + '\'' +
            '}';
   }
}
