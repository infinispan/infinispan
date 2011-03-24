package org.infinispan.server.core

import org.infinispan.lifecycle.AbstractModuleLifecycle
import org.infinispan.factories.GlobalComponentRegistry
import org.infinispan.config.GlobalConfiguration

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.Externalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
class LifecycleCallbacks extends AbstractModuleLifecycle {

   override def cacheManagerStarting(gcr: GlobalComponentRegistry, gc: GlobalConfiguration) = addExternalizer(gc)

   private[core] def addExternalizer(globalCfg : GlobalConfiguration) =
      globalCfg.addExternalizer(ExternalizerIds.SERVER_CACHE_VALUE, new CacheValue.Externalizer)
}