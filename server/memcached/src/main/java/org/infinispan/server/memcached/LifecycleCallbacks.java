package org.infinispan.server.memcached;

import static org.infinispan.server.core.ExternalizerIds.MEMCACHED_METADATA;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.CompatibilityModeConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
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


   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      Configuration configuration = cr.getComponent(Configuration.class);
      CompatibilityModeConfiguration compatibility = configuration.compatibility();
      if (compatibility.enabled()) {
         Marshaller marshaller = compatibility.marshaller();
         cr.getEncoderRegistry().registerEncoder(new MemcachedCompatEncoder(marshaller));
      }
   }
}
