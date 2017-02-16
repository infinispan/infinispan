package org.infinispan.server.core;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.lifecycle.AbstractModuleLifecycle;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.commons.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class LifecycleCallbacks extends AbstractModuleLifecycle {

   static ComponentMetadataRepo componentMetadataRepo;

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      componentMetadataRepo = gcr.getComponentMetadataRepo();
   }

   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
      configuration.storeAsBinary().enabled(false);
   }
}
