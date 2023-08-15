package org.infinispan.server.resp;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.server.resp.commands.tx.WATCH;
import org.infinispan.server.resp.filter.EventListenerKeysFilter;

@InfinispanModule(name = "resp", requiredModules = "core")
public class RespModuleLifecycle implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      final Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalConfiguration.serialization()
            .advancedExternalizers();

      externalizerMap.put(EventListenerKeysFilter.EXTERNALIZER.getId(), EventListenerKeysFilter.EXTERNALIZER);
      externalizerMap.put(WATCH.EXTERNALIZER.getId(), WATCH.EXTERNALIZER);
   }
}
