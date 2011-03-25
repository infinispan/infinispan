package org.infinispan.server.hotrod

import org.infinispan.lifecycle.AbstractModuleLifecycle
import org.infinispan.factories.GlobalComponentRegistry
import org.infinispan.server.core.ExternalizerIds
import org.infinispan.config.GlobalConfiguration

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.Externalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
class LifecycleCallbacks extends AbstractModuleLifecycle {

   override def cacheManagerStarting(gcr: GlobalComponentRegistry, globalCfg: GlobalConfiguration) {
      globalCfg.fluent.serialization
         .addExternalizer(ExternalizerIds.TOPOLOGY_ADDRESS, new TopologyAddress.Externalizer)
         .addExternalizer(ExternalizerIds.TOPOLOGY_VIEW, new TopologyView.Externalizer)
   }

}