package org.infinispan.rest;

import static org.infinispan.marshall.protostream.impl.SerializationContextRegistry.MarshallerType.GLOBAL;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;

/**
 * Lifecycle callbacks for the REST module. Register the externalizers to serialize the objects in package
 * {@link org.infinispan.rest.distribution} for querying data distribution.
 *
 * @since 14.0
 */
@InfinispanModule(name = "server-rest")
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(GLOBAL, new GlobalContextInitializerImpl());
   }
}
