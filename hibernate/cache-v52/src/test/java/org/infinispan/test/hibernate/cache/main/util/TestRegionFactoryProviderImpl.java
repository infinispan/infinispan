package org.infinispan.test.hibernate.cache.main.util;

import java.util.Properties;

import org.hibernate.Cache;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.engine.spi.CacheImplementor;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.main.InfinispanRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.infinispan.test.hibernate.cache.main.functional.cluster.ClusterAwareRegionFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(TestRegionFactoryProvider.class)
public class TestRegionFactoryProviderImpl implements TestRegionFactoryProvider {
   @Override
   public Class<? extends RegionFactory> getRegionFactoryClass() {
      return TestInfinispanRegionFactory.class;
   }

   @Override
   public Class<? extends RegionFactory> getClusterAwareClass() {
      return ClusterAwareRegionFactory.class;
   }

   @Override
   public TestRegionFactory create(Properties properties) {
      return new TestRegionFactoryImpl(new TestInfinispanRegionFactory(properties));
   }

   @Override
   public TestRegionFactory wrap(RegionFactory regionFactory) {
      return new TestRegionFactoryImpl((InfinispanRegionFactory) regionFactory);
   }

   @Override
   public TestRegionFactory findRegionFactory(Cache cache) {
      return new TestRegionFactoryImpl((InfinispanRegionFactory) ((CacheImplementor) cache).getRegionFactory());
   }

   @Override
   public InfinispanBaseRegion findTimestampsRegion(Cache cache) {
      return (InfinispanBaseRegion) ((CacheImplementor) cache).getUpdateTimestampsCache().getRegion();
   }

   @Override
   public boolean supportTransactionalCaches() {
      return true;
   }
}
