package org.infinispan.test.hibernate.cache.main.util;

import java.util.Properties;

import org.hibernate.boot.internal.SessionFactoryBuilderImpl;
import org.hibernate.boot.internal.SessionFactoryOptionsImpl;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.cache.internal.CacheDataDescriptionImpl;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.internal.util.compare.ComparableComparator;
import org.hibernate.service.ServiceRegistry;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.main.InfinispanRegionFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;

class TestRegionFactoryImpl implements TestRegionFactory {
   private static final CacheDataDescriptionImpl MUTABLE_VERSIONED =
         new CacheDataDescriptionImpl(true, true, ComparableComparator.INSTANCE, null);

   private final InfinispanRegionFactory delegate;

   TestRegionFactoryImpl(InfinispanRegionFactory delegate) {
      this.delegate = delegate;
   }

   @Override
   public void start(ServiceRegistry serviceRegistry, Properties p) {
      final SessionFactoryOptionsImpl sessionFactoryOptions = new SessionFactoryOptionsImpl(
            new SessionFactoryBuilderImpl.SessionFactoryOptionsStateStandardImpl(
                  (StandardServiceRegistry) serviceRegistry
            )
      );

      delegate.start(sessionFactoryOptions, p);
   }

   @Override
   public void stop() {
      delegate.stop();
   }

   @Override
   public void setCacheManager(EmbeddedCacheManager cm) {
      delegate.setCacheManager(cm);
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return delegate.getCacheManager();
   }

   @Override
   public String getBaseConfiguration(String regionName) {
      if (delegate instanceof TestInfinispanRegionFactory) {
         return ((TestInfinispanRegionFactory) delegate).getBaseConfiguration(regionName);
      }
      throw new UnsupportedOperationException();
   }

   @Override
   public Configuration getConfigurationOverride(String regionName) {
      if (delegate instanceof TestInfinispanRegionFactory) {
         return ((TestInfinispanRegionFactory) delegate).getConfigurationOverride(regionName);
      }
      throw new UnsupportedOperationException();
   }

   @Override
   public Configuration getPendingPutsCacheConfiguration() {
      return delegate.getPendingPutsCacheConfiguration();
   }

   @Override
   public InfinispanBaseRegion buildCollectionRegion(String regionName, AccessType accessType) {
      String prefix = delegate.getSettings().getCacheRegionPrefix();
      if (prefix != null && !prefix.isEmpty()) regionName = prefix + '.' + regionName;
      return (InfinispanBaseRegion) delegate.buildCollectionRegion(regionName, (Properties) null, MUTABLE_VERSIONED);
   }

   @Override
   public InfinispanBaseRegion buildEntityRegion(String regionName, AccessType accessType) {
      String prefix = delegate.getSettings().getCacheRegionPrefix();
      if (prefix != null && !prefix.isEmpty()) regionName = prefix + '.' + regionName;
      return (InfinispanBaseRegion) delegate.buildEntityRegion(regionName, (Properties) null, MUTABLE_VERSIONED);
   }

   @Override
   public InfinispanBaseRegion buildTimestampsRegion(String regionName) {
      String prefix = delegate.getSettings().getCacheRegionPrefix();
      if (prefix != null && !prefix.isEmpty()) regionName = prefix + '.' + regionName;
      return (InfinispanBaseRegion) delegate.buildTimestampsRegion(regionName, (Properties) null);
   }

   @Override
   public InfinispanBaseRegion buildQueryResultsRegion(String regionName) {
      String prefix = delegate.getSettings().getCacheRegionPrefix();
      if (prefix != null && !prefix.isEmpty()) regionName = prefix + '.' + regionName;
      return (InfinispanBaseRegion) delegate.buildQueryResultsRegion(regionName, (Properties) null);
   }

   @Override
   public long nextTimestamp() {
      return delegate.nextTimestamp();
   }
}
