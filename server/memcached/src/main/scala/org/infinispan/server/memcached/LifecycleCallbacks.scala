package org.infinispan.server.memcached

import org.infinispan.lifecycle.AbstractModuleLifecycle
import org.infinispan.factories.GlobalComponentRegistry
import org.infinispan.server.core.ExternalizerIds

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.Externalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
class LifecycleCallbacks extends AbstractModuleLifecycle {

   override def cacheManagerStarting(gcr: GlobalComponentRegistry) =
      gcr.getGlobalConfiguration.addExternalizer(ExternalizerIds.MEMCACHED_CACHE_VALUE, new MemcachedValue.Externalizer)

}