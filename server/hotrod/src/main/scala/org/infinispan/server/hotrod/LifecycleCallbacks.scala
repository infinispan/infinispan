package org.infinispan.server.hotrod

import org.infinispan.lifecycle.AbstractModuleLifecycle
import org.infinispan.factories.GlobalComponentRegistry
import org.infinispan.server.core.ExternalizerIds._
import org.infinispan.configuration.global.GlobalConfiguration

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
class LifecycleCallbacks extends AbstractModuleLifecycle {

   override def cacheManagerStarting(gcr: GlobalComponentRegistry, globalCfg: GlobalConfiguration) =
      globalCfg.serialization().advancedExternalizers().put(
            SERVER_ADDRESS, new ServerAddress.Externalizer)

}
