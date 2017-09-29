package org.infinispan.server.hotrod;

import static org.infinispan.server.core.ExternalizerIds.BINARY_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.BINARY_FILTER;
import static org.infinispan.server.core.ExternalizerIds.BINARY_FILTER_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.ITERATION_FILTER;
import static org.infinispan.server.core.ExternalizerIds.KEY_VALUE_VERSION_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.KEY_VALUE_WITH_PREVIOUS_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.SERVER_ADDRESS;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.server.hotrod.ClientListenerRegistry.UnmarshallConverterExternalizer;
import org.infinispan.server.hotrod.ClientListenerRegistry.UnmarshallFilterConverterExternalizer;
import org.infinispan.server.hotrod.ClientListenerRegistry.UnmarshallFilterExternalizer;
import org.infinispan.server.hotrod.event.KeyValueWithPreviousEventConverterExternalizer;
import org.infinispan.server.hotrod.iteration.IterationFilter;

/**
 * Module lifecycle callbacks implementation that enables module specific {@link org.infinispan.marshall.AdvancedExternalizer}
 * implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class LifecycleCallbacks extends AbstractModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      Map<Integer, AdvancedExternalizer<?>> externalizers = globalCfg.serialization().advancedExternalizers();
      externalizers.put(SERVER_ADDRESS, new ServerAddress.Externalizer());
      externalizers.put(BINARY_FILTER, new UnmarshallFilterExternalizer());
      externalizers.put(BINARY_CONVERTER, new UnmarshallConverterExternalizer());
      externalizers.put(KEY_VALUE_VERSION_CONVERTER, new KeyValueVersionConverter.Externalizer());
      externalizers.put(BINARY_FILTER_CONVERTER, new UnmarshallFilterConverterExternalizer());
      externalizers.put(KEY_VALUE_WITH_PREVIOUS_CONVERTER, new KeyValueWithPreviousEventConverterExternalizer());
      externalizers.put(ITERATION_FILTER, new IterationFilter.IterationFilterExternalizer());
   }

}
