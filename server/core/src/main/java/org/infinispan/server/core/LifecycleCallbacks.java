package org.infinispan.server.core;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.lifecycle.ModuleLifecycle;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.commons.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class LifecycleCallbacks implements ModuleLifecycle {

   static ComponentMetadataRepo componentMetadataRepo;

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      componentMetadataRepo = gcr.getComponentMetadataRepo();
   }
}
