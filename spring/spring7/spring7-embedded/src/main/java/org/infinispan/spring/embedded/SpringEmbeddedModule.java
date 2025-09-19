package org.infinispan.spring.embedded;

import static org.infinispan.marshall.protostream.impl.SerializationContextRegistry.MarshallerType.GLOBAL;
import static org.infinispan.marshall.protostream.impl.SerializationContextRegistry.MarshallerType.PERSISTENCE;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.util.NullValue;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.spring.common.session.MapSessionProtoAdapter;
import org.springframework.session.MapSession;

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
      ClassAllowList serializationAllowList = gcr.getCacheManager().getClassAllowList();
      serializationAllowList.addClasses(NullValue.class);
      serializationAllowList.addRegexps("java.util\\..*", "org.springframework\\..*");
      JavaSerializationMarshaller serializationMarshaller = new JavaSerializationMarshaller(serializationAllowList);

      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      addSessionContextInitializerAndMarshaller(ctxRegistry, serializationMarshaller);
   }

   private void addSessionContextInitializerAndMarshaller(SerializationContextRegistry ctxRegistry,
                                                          JavaSerializationMarshaller serializationMarshaller) {
      // Skip registering the marshallers if the MapSession class is not available
      try {
         new MapSession();
      } catch (NoClassDefFoundError e) {
         Log.CONFIG.debug("spring-session classes not found, skipping the session context initializer registration");
         return;
      }

      org.infinispan.spring.common.session.PersistenceContextInitializerImpl sessionSci = new org.infinispan.spring.common.session.PersistenceContextInitializerImpl();
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, sessionSci);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, sessionSci);

      BaseMarshaller<?> sessionAttributeMarshaller = new MapSessionProtoAdapter.SessionAttributeRawMarshaller(serializationMarshaller);
      ctxRegistry.addMarshaller(PERSISTENCE, sessionAttributeMarshaller);
      ctxRegistry.addMarshaller(GLOBAL, sessionAttributeMarshaller);
   }
}
