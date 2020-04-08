package org.infinispan.server.logging.events;

import static org.infinispan.marshall.protostream.impl.SerializationContextRegistry.MarshallerType;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;

/**
 * LifecycleCallback for the server event logger module. Registers advanced externalizers and
 * initializes the server logger
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@InfinispanModule(name = "server-event-logger", requiredModules = {"core", "query-core"})
public class LifecycleCallbacks implements ModuleLifecycle {

   private EventLogger oldEventLogger;

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration gc) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());

      EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      InternalCacheRegistry internalCacheRegistry = gcr.getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache(ServerEventLogger.EVENT_LOG_CACHE, getTaskHistoryCacheConfiguration(cacheManager).build(),
            EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT, InternalCacheRegistry.Flag.QUERYABLE));
      // Install the new logger component
      oldEventLogger = gcr.getComponent(EventLogManager.class).replaceEventLogger(new ServerEventLogger(cacheManager, gcr.getTimeService()));
   }

   @Override
   public void cacheStopping(ComponentRegistry cr, String cacheName) {
      // Replace the event logger when the cache is stopping, not later when the global components are stopping
      if (cacheName.equals(ServerEventLogger.EVENT_LOG_CACHE)) {
         cr.getComponent(EventLogManager.class).replaceEventLogger(oldEventLogger);
      }
   }

   private ConfigurationBuilder getTaskHistoryCacheConfiguration(EmbeddedCacheManager cacheManager) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.memory().size(100L).persistence().passivation(true).expiration().lifespan(7, TimeUnit.DAYS);
      return cfg;
   }
}
