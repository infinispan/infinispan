package org.infinispan.marshall.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.NamedMarshallerConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Implementation of {@link MarshallerRegistry}.
 *
 * @author William Burns
 * @since 16.2
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class MarshallerRegistryImpl implements MarshallerRegistry {

   @Inject GlobalConfiguration globalConfiguration;
   @Inject EmbeddedCacheManager cacheManager;
   @Inject GlobalComponentRegistry globalComponentRegistry;

   private final Map<String, Marshaller> marshallers = new ConcurrentHashMap<>();
   private volatile Marshaller defaultMarshaller;

   @Start
   public void start() {
      // Register all named marshallers from configuration - don't register default yet

      // Register all named marshallers from configuration
      for (NamedMarshallerConfiguration config : globalConfiguration.serialization().namedMarshallers()) {
         String name = config.name();
         Marshaller marshaller = config.marshaller();

         if (marshaller == null) {
            // Instantiate from class name
            String className = config.marshallerClass();
            if (className == null || className.isEmpty()) {
               throw new IllegalStateException("Named marshaller '" + name + "' has neither a marshaller instance nor a class name");
            }

            ClassLoader classLoader = globalConfiguration.classLoader();
            marshaller = Util.getInstance(className, classLoader);
         }

         // Initialize the marshaller with the cache manager's class allow list
         marshaller.initialize(cacheManager.getClassAllowList());

         // Check for duplicates
         if (marshallers.containsKey(name)) {
            throw new IllegalArgumentException("A marshaller with name '" + name + "' is already registered");
         }

         marshallers.put(name, marshaller);
      }
   }

   @Override
   public Marshaller getMarshaller(String name) {
      if (name == null || name.isEmpty()) {
         throw new IllegalArgumentException("Marshaller name cannot be null or empty");
      }

      // Special handling for the default marshaller - retrieve it lazily
      if (DEFAULT_MARSHALLER_NAME.equals(name)) {
         if (defaultMarshaller == null) {
            synchronized (this) {
               if (defaultMarshaller == null) {
                  defaultMarshaller = globalComponentRegistry.getComponent(Marshaller.class, KnownComponentNames.USER_MARSHALLER);
               }
            }
         }
         return defaultMarshaller;
      }

      return marshallers.get(name);
   }

   @Override
   public boolean hasMarshaller(String name) {
      if (DEFAULT_MARSHALLER_NAME.equals(name)) {
         return true; // Default marshaller is always available
      }
      return marshallers.containsKey(name);
   }
}
