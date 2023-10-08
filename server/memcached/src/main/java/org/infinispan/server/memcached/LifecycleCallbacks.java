package org.infinispan.server.memcached;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;

/**
 * @author Galder Zamarreño
 * @since 5.0
 */
@InfinispanModule(name = "server-memcached", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {
   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      PersistenceContextInitializerImpl sci = new PersistenceContextInitializerImpl();
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, sci);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, sci);
   }
}
