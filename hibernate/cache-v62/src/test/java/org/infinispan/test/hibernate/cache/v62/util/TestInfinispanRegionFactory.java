package org.infinispan.test.hibernate.cache.v62.util;

import java.util.Collection;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

import org.hibernate.service.ServiceRegistry;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.hibernate.cache.commons.DataType;
import org.infinispan.hibernate.cache.commons.DefaultCacheManagerProvider;
import org.infinispan.hibernate.cache.v62.InfinispanRegionFactory;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.hibernate.cache.commons.util.TestConfigurationHook;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;

/**
 * Factory that should be overridden in tests.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class TestInfinispanRegionFactory extends InfinispanRegionFactory {
   private final TimeService timeService;
   private final TestConfigurationHook configurationHook;
   private final EmbeddedCacheManager providedManager;
   private final Consumer<EmbeddedCacheManager> afterManagerCreated;
   private final Function<AdvancedCache, AdvancedCache> wrapCache;

   public TestInfinispanRegionFactory(Properties properties) {
      timeService = (TimeService) properties.getOrDefault(TestRegionFactory.TIME_SERVICE, null);
      providedManager = (EmbeddedCacheManager) properties.getOrDefault(TestRegionFactory.MANAGER, null);
      afterManagerCreated = (Consumer<EmbeddedCacheManager>) properties.getOrDefault(TestRegionFactory.AFTER_MANAGER_CREATED, null);
      wrapCache = (Function<AdvancedCache, AdvancedCache>) properties.getOrDefault(TestRegionFactory.WRAP_CACHE, null);
      Class<TestConfigurationHook> hookClass = (Class<TestConfigurationHook>) properties.getOrDefault(TestRegionFactory.CONFIGURATION_HOOK, TestConfigurationHook.class);
      try {
         configurationHook = hookClass.getConstructor(Properties.class).newInstance(properties);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager(Properties properties, ServiceRegistry serviceRegistry) {
      // If the cache manager has been provided by calling setCacheManager, don't create a new one
      EmbeddedCacheManager cacheManager = getCacheManager();
      if (cacheManager != null) {
         return cacheManager;
      } else if (providedManager != null) {
         cacheManager = providedManager;
      } else {
         ConfigurationBuilderHolder holder = DefaultCacheManagerProvider.loadConfiguration(serviceRegistry, properties);
         configurationHook.amendConfiguration(holder);
         cacheManager = new DefaultCacheManager(holder, true);
      }
      if (afterManagerCreated != null) {
         afterManagerCreated.accept(cacheManager);
      }
      if (timeService != null) {
         BasicComponentRegistry basicComponentRegistry = GlobalComponentRegistry.componentOf(cacheManager, BasicComponentRegistry.class);
         basicComponentRegistry.replaceComponent(TimeService.class.getName(), timeService, false);
         basicComponentRegistry.rewire();
      }
      return cacheManager;
   }

   @Override
   protected AdvancedCache getCache(String cacheName, String unqualifiedRegionName, DataType type, Collection<String> legacyUnqualifiedNames) {
      AdvancedCache cache = super.getCache(cacheName, unqualifiedRegionName, type, legacyUnqualifiedNames);
      return wrapCache == null ? cache : wrapCache.apply(cache);
   }

   @Override
   public long nextTimestamp() {
      if (timeService == null) {
         return super.nextTimestamp();
      } else {
         return timeService.wallClockTime();
      }
   }

   /* Used for testing */
   public String getBaseConfiguration(String regionName) {
      return baseConfigurations.get(regionName);
   }

   /* Used for testing */
   public Configuration getConfigurationOverride(String regionName) {
      return configOverrides.get(regionName).build(false);
   }
}
