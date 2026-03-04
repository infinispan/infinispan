package org.infinispan.topology;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.util.Immutables;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidResponseCollector;

/**
 * Collects confirmations when restoring stable topology after a graceful shutdown.
 *
 * <p>
 * When a stable topology with {@link CacheTopology#wasTopologyRestoredFromState()} is broadcast, this collector tracks
 * which target nodes have acknowledged the update. Nodes that respond successfully are removed from the pending set; nodes
 * that are unreachable or respond with an error remain pending.
 * </p>
 *
 * <p>
 * The resulting {@link StableTopologyConfirmationResult} provides the set of nodes that did not confirm, which is used
 * by {@link ClusterCacheStatus} to delay rebalancing until all expected members have installed the restored topology.
 * </p>
 *
 * @since 16.2
 * @author José Bolina
 */
final class StableTopologyConfirmationCollector extends ValidResponseCollector<StableTopologyConfirmationCollector.StableTopologyConfirmationResult> {

   private final Set<Address> targets;

   public StableTopologyConfirmationCollector(Collection<Address> targets) {
      this.targets = new HashSet<>(targets);
   }

   @Override
   public StableTopologyConfirmationResult finish() {
      return new StableTopologyConfirmationResult(Immutables.immutableSetWrap(targets));
   }

   @Override
   protected StableTopologyConfirmationResult addValidResponse(Address sender, ValidResponse<?> response) {
      targets.remove(sender);
      return null;
   }

   @Override
   protected StableTopologyConfirmationResult addTargetNotFound(Address sender) {
      // Do nothing, keep the sender as still pending confirmation.
      return null;
   }

   @Override
   protected StableTopologyConfirmationResult addException(Address sender, Exception exception) {
      // Do nothing, keep the sender as still pending confirmation.
      return null;
   }

   public record StableTopologyConfirmationResult(Set<Address> pending) {
      public boolean allNodesConfirmed() {
         return pending.isEmpty();
      }
   }
}
