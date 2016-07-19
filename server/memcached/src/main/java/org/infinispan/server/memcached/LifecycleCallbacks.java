package org.infinispan.server.memcached;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.AbstractModuleLifecycle;

import static org.infinispan.server.core.ExternalizerIds.MEMCACHED_METADATA;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class LifecycleCallbacks extends AbstractModuleLifecycle {
   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      globalConfiguration.serialization().advancedExternalizers().put(MEMCACHED_METADATA,
              new MemcachedMetadataExternalizer());
   }
}