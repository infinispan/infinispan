package org.infinispan.test.hibernate.cache.commons.util;

import java.util.Properties;
import java.util.ServiceLoader;

import org.hibernate.Cache;
import org.hibernate.cache.spi.RegionFactory;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;

public interface TestRegionFactoryProvider {
   TestRegionFactoryProvider INSTANCE = ServiceLoader.load(TestRegionFactoryProvider.class).iterator().next();

   static TestRegionFactoryProvider load() {
      return INSTANCE;
   }

   Class<? extends RegionFactory> getRegionFactoryClass();

   Class<? extends RegionFactory> getClusterAwareClass();

   TestRegionFactory create(Properties properties);

   TestRegionFactory wrap(RegionFactory regionFactory);

   TestRegionFactory findRegionFactory(Cache cache);

   InfinispanBaseRegion findTimestampsRegion(Cache cache);

   boolean supportTransactionalCaches();
}
