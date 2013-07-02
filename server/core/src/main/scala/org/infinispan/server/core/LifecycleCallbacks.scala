package org.infinispan.server.core

import org.infinispan.lifecycle.AbstractModuleLifecycle
import org.infinispan.server.core.ExternalizerIds._
import org.infinispan.factories.{ComponentRegistry, GlobalComponentRegistry}
import org.infinispan.configuration.global.GlobalConfiguration
import org.infinispan.configuration.cache.Configuration
import org.infinispan.factories.components.ComponentMetadataRepo

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
class LifecycleCallbacks extends AbstractModuleLifecycle {

   override def cacheManagerStarting(gcr: GlobalComponentRegistry, globalCfg: GlobalConfiguration) {
      LifecycleCallbacks.componentMetadataRepo = gcr.getComponentMetadataRepo
   }

   override def cacheStarting(cr: ComponentRegistry, cfg: Configuration, cacheName: String) {
      cfg.storeAsBinary().enabled(false)
   }

}

object LifecycleCallbacks {

   var componentMetadataRepo: ComponentMetadataRepo = _

}