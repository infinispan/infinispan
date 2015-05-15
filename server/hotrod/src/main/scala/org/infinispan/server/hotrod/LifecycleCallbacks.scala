package org.infinispan.server.hotrod

import org.infinispan.lifecycle.AbstractModuleLifecycle
import org.infinispan.factories.GlobalComponentRegistry
import org.infinispan.server.core.ExternalizerIds._
import org.infinispan.configuration.global.GlobalConfiguration
import org.infinispan.server.hotrod.ClientListenerRegistry.{UnmarshallFilterConverterExternalizer, UnmarshallConverterExternalizer, UnmarshallFilterExternalizer}
import org.infinispan.server.hotrod.KeyValueVersionConverterFactory.KeyValueVersionConverter
import org.infinispan.server.hotrod.event.KeyValueWithPreviousEventConverterExternalizer
import org.infinispan.server.hotrod.iteration._

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
      externalizers.put(BINARY_FILTER, new UnmarshallFilterExternalizer())
      externalizers.put(BINARY_CONVERTER, new UnmarshallConverterExternalizer())
      externalizers.put(KEY_VALUE_VERSION_CONVERTER, new KeyValueVersionConverter.Externalizer())
      externalizers.put(BINARY_FILTER_CONVERTER, new UnmarshallFilterConverterExternalizer())
      externalizers.put(KEY_VALUE_WITH_PREVIOUS_CONVERTER, new KeyValueWithPreviousEventConverterExternalizer())
      externalizers.put(ITERATION_FILTER, new IterationFilterExternalizer())
   }

}
