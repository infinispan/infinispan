package org.infinispan.persistence.remote;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;

/**
 * @author gustavonalle
 * @since 8.2
 */
@InfinispanModule(name = "cachestore-remote", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, new GlobalContextInitializerImpl());
   }
}
