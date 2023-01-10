package org.infinispan.test.hibernate.cache.v62.util;

import java.util.Properties;

import org.hibernate.Cache;
import org.hibernate.cache.spi.RegionFactory;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.v62.InfinispanRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.infinispan.test.hibernate.cache.v62.functional.cluster.ClusterAwareRegionFactory;
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
      return new TestRegionFactoryImpl((InfinispanRegionFactory) ((org.hibernate.cache.spi.CacheImplementor) cache).getRegionFactory());
   }

   @Override
   public InfinispanBaseRegion findTimestampsRegion(Cache cache) {
      return (InfinispanBaseRegion) ((org.hibernate.cache.spi.CacheImplementor) cache).getTimestampsCache().getRegion();
   }

   @Override
   public boolean supportTransactionalCaches() {
      return false;
   }
}
