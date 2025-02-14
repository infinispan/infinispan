package org.infinispan.server.resp;

import static org.infinispan.server.core.ExternalizerIds.ITERATION_FILTER;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.server.iteration.IterationFilter;
import org.infinispan.server.resp.commands.tx.WATCH;
import org.infinispan.server.resp.filter.ComposedFilterConverter;
import org.infinispan.server.resp.filter.EventListenerConverter;
import org.infinispan.server.resp.filter.EventListenerKeysFilter;
import org.infinispan.server.resp.json.JsonDelFunction;
import org.infinispan.server.resp.json.JsonAppendFunction;
import org.infinispan.server.resp.json.JsonGetFunction;
import org.infinispan.server.resp.json.JsonSetFunction;
import org.infinispan.server.resp.json.JsonLenFunction;
import org.infinispan.server.resp.json.JsonObjkeysFunction;
import org.infinispan.server.resp.json.JsonToggleFunction;
import org.infinispan.server.resp.json.JsonTypeFunction;

@InfinispanModule(name = "resp", requiredModules = "core")
public class RespModuleLifecycle implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      final Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalConfiguration.serialization()
            .advancedExternalizers();

      externalizerMap.put(EventListenerKeysFilter.EXTERNALIZER.getId(), EventListenerKeysFilter.EXTERNALIZER);
      externalizerMap.put(WATCH.EXTERNALIZER.getId(), WATCH.EXTERNALIZER);
      externalizerMap.put(EventListenerConverter.EXTERNALIZER.getId(), EventListenerConverter.EXTERNALIZER);
      externalizerMap.put(ComposedFilterConverter.EXTERNALIZER.getId(), ComposedFilterConverter.EXTERNALIZER);
      externalizerMap.put(JsonGetFunction.EXTERNALIZER.getId(), JsonGetFunction.EXTERNALIZER);
      externalizerMap.put(JsonSetFunction.EXTERNALIZER.getId(), JsonSetFunction.EXTERNALIZER);
      externalizerMap.put(JsonLenFunction.EXTERNALIZER.getId(), JsonLenFunction.EXTERNALIZER);
      externalizerMap.put(JsonTypeFunction.EXTERNALIZER.getId(), JsonTypeFunction.EXTERNALIZER);
      externalizerMap.put(JsonDelFunction.EXTERNALIZER.getId(), JsonDelFunction.EXTERNALIZER);
      externalizerMap.put(JsonAppendFunction.EXTERNALIZER.getId(), JsonAppendFunction.EXTERNALIZER);
      externalizerMap.put(JsonToggleFunction.EXTERNALIZER.getId(), JsonToggleFunction.EXTERNALIZER);
      externalizerMap.put(JsonObjkeysFunction.EXTERNALIZER.getId(), JsonObjkeysFunction.EXTERNALIZER);

      // Externalizer that could be loaded by other modules.
      externalizerMap.put(ITERATION_FILTER, new IterationFilter.IterationFilterExternalizer());

      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, new PersistenceContextInitializerImpl());
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());
   }
}
