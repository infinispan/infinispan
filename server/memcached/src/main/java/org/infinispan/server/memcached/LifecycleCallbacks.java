package org.infinispan.server.memcached;

import static org.infinispan.server.core.ExternalizerIds.MEMCACHED_METADATA;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.commons.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class LifecycleCallbacks implements ModuleLifecycle {
   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      globalConfiguration.serialization().advancedExternalizers().put(MEMCACHED_METADATA,
            new MemcachedMetadataExternalizer());
   }
}
