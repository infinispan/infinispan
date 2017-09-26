package org.infinispan.server.eventlogger;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;
import org.kohsuke.MetaInfServices;

/**
 * LifecycleCallback for the server event logger module. Registers advanced externalizers and
 * initializes the server logger
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@MetaInfServices(ModuleLifecycle.class)
public class LifecycleCallbacks implements ModuleLifecycle {

   private EventLogger oldEventLogger;

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration gc) {
      // Add the externalizer for server events
      Map<Integer, AdvancedExternalizer<?>> externalizerMap = gc.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.SERVER_EVENT, new ServerEventImpl.Externalizer());
      // Create the event log cache configuration in the internal cache registry
      EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      InternalCacheRegistry internalCacheRegistry = gcr.getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache(ServerEventLogger.EVENT_LOG_CACHE, getTaskHistoryCacheConfiguration(cacheManager).build(),
            EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT, InternalCacheRegistry.Flag.QUERYABLE));
      // Install the new logger component
      oldEventLogger = gcr.getComponent(EventLogManager.class).replaceEventLogger(new ServerEventLogger(cacheManager, gcr.getTimeService()));
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      gcr.getComponent(EventLogManager.class).replaceEventLogger(oldEventLogger);
   }

   private ConfigurationBuilder getTaskHistoryCacheConfiguration(EmbeddedCacheManager cacheManager) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.eviction().size(100l).persistence().passivation(true).expiration().lifespan(7, TimeUnit.DAYS);
      return cfg;
   }
}
