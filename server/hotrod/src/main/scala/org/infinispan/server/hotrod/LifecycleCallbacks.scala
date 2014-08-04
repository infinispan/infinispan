package org.infinispan.server.hotrod

import org.infinispan.lifecycle.AbstractModuleLifecycle
import org.infinispan.factories.GlobalComponentRegistry
import org.infinispan.server.core.ExternalizerIds._
import org.infinispan.configuration.global.GlobalConfiguration
import org.infinispan.server.hotrod.ClientListenerRegistry.{BinaryConverterExternalizer, BinaryFilterExternalizer}

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
class LifecycleCallbacks extends AbstractModuleLifecycle {

   override def cacheManagerStarting(gcr: GlobalComponentRegistry, globalCfg: GlobalConfiguration) = {
      val externalizers = globalCfg.serialization().advancedExternalizers()
      externalizers.put(SERVER_ADDRESS, new ServerAddress.Externalizer)
      externalizers.put(BINARY_FILTER, new BinaryFilterExternalizer())
      externalizers.put(BINARY_CONVERTER, new BinaryConverterExternalizer())
   }

}
