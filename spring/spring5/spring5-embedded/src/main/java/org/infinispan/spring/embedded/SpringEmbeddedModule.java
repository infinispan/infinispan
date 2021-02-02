package org.infinispan.spring.embedded;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.spring.common.PersistenceContextInitializerImpl;
import org.infinispan.spring.common.provider.NullValue;

/**
 * Add support for Spring-specific classes like {@link NullValue} in embedded caches.
 *
 * @author Dan Berindei
 * @since 12.1
 */
@InfinispanModule(name = "spring-embedded", requiredModules = "core")
public class SpringEmbeddedModule implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      PersistenceContextInitializerImpl sci = new PersistenceContextInitializerImpl();
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, sci);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, sci);

      gcr.getCacheManager().getClassAllowList().addClasses(NullValue.class);
   }
}
