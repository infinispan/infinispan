package org.infinispan.topology;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.topology.CapacityFactorUpdateCommand;
import org.infinispan.commons.configuration.attributes.AttributeValidator;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;

final class CapacityFactorRuntimeAttributeValidator implements AttributeValidator<Float> {
   private final String cacheName;
   private final GlobalConfiguration globalConfiguration;
   private final Configuration cacheConfiguration;
   private final TopologyManagementHelper helper;
   private final Transport transport;

   CapacityFactorRuntimeAttributeValidator(
         String cacheName,
         GlobalConfiguration globalConfiguration,
         Configuration cacheConfiguration,
         TopologyManagementHelper helper,
         Transport transport) {
      this.cacheName = cacheName;
      this.globalConfiguration = globalConfiguration;
      this.cacheConfiguration = cacheConfiguration;
      this.helper = helper;
      this.transport = transport;
   }

   @Override
   public void validate(Float value) {
      CacheMode mode = cacheConfiguration.clustering().cacheMode();

      // We simply ignore update of non-clustered caches.
      // That is, a local cache will always hold all data.
      if (!mode.isClustered()) {
         return;
      }

      if (globalConfiguration.isZeroCapacityNode() && value > 0) {
         throw Log.CONFIG.capacityFactorUpdateOnZeroCapacityNode();
      }

      if (!mode.isDistributed() && value != 0f && value != 1f)
         throw Log.CONFIG.capacityFactorNonBinarySkipped(cacheName, mode, value);

      ReplicableCommand command = new CapacityFactorUpdateCommand(transport.getAddress(), cacheName, value);
      CompletionStages.join(helper.executeOnCoordinator(transport, command, getGlobalRpcTimeout()));
   }

   private int getGlobalRpcTimeout() {
      return (int) globalConfiguration.transport().distributedSyncTimeout();
   }
}
