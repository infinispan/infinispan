package org.infinispan.test.hibernate.cache.commons.util;

import java.util.Properties;

import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.service.ServiceRegistry;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.hibernate.cache.commons.TimeSource;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.manager.EmbeddedCacheManager;

public interface TestRegionFactory extends TimeSource {
   String PREFIX = TestRegionFactory.class.getName() + ".";
   String PENDING_PUTS_SIMPLE = PREFIX + "pendingPuts.simple";
   String CACHE_MODE = PREFIX + "cacheMode";
   String TRANSACTIONAL = PREFIX + "transactional";
   String TIME_SERVICE = PREFIX + "timeService";
   String MANAGER = PREFIX + "manager";
   String AFTER_MANAGER_CREATED = PREFIX + "afterManagerCreated";
   String WRAP_CACHE = PREFIX + "wrap.cache";
   String CONFIGURATION_HOOK = PREFIX + "configuration.hook";

   void start(ServiceRegistry serviceRegistry, Properties p);

   void stop();

   void setCacheManager(EmbeddedCacheManager cm);

   EmbeddedCacheManager getCacheManager();

   String getBaseConfiguration(String regionName);

   Configuration getConfigurationOverride(String regionName);

   Configuration getPendingPutsCacheConfiguration();

   InfinispanBaseRegion buildCollectionRegion(String regionName, AccessType accessType);

   InfinispanBaseRegion buildEntityRegion(String regionName, AccessType accessType);

   InfinispanBaseRegion buildTimestampsRegion(String regionName);

   InfinispanBaseRegion buildQueryResultsRegion(String regionName);

   RegionFactory unwrap();
}
