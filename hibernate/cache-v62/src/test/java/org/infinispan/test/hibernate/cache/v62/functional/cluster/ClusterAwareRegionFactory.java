package org.infinispan.test.hibernate.cache.v62.functional.cluster;

import java.util.Map;
import java.util.Properties;

import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext;
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig;
import org.hibernate.cache.spi.CacheTransactionSynchronization;
import org.hibernate.cache.spi.DomainDataRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.hibernate.cache.v62.InfinispanRegionFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.hibernate.cache.commons.functional.cluster.ClusterAware;
import org.infinispan.test.hibernate.cache.commons.functional.cluster.DualNodeTest;
import org.infinispan.test.hibernate.cache.commons.util.CacheTestUtil;

/**
 * ClusterAwareRegionFactory.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class ClusterAwareRegionFactory implements RegionFactory {

   private final InfinispanRegionFactory delegate;
   private String cacheManagerName;
   private boolean locallyAdded;

   public ClusterAwareRegionFactory(Properties props) {
      Class<? extends InfinispanRegionFactory> regionFactoryClass =
            (Class<InfinispanRegionFactory>) props.get(DualNodeTest.REGION_FACTORY_DELEGATE);
      delegate = CacheTestUtil.createRegionFactory(regionFactoryClass, props);
   }

   @Override
   public void start(SessionFactoryOptions settings, Map configValues) throws CacheException {
      cacheManagerName = (String) configValues.get(DualNodeTest.NODE_ID_PROP);

      EmbeddedCacheManager existing = ClusterAware.getCacheManager(cacheManagerName);
      locallyAdded = (existing == null);

      if (locallyAdded) {
         delegate.start(settings, configValues);
         ClusterAware.addCacheManager(cacheManagerName, delegate.getCacheManager());
      } else {
         delegate.setCacheManager(existing);
      }
   }

   public void stop() {
      if (locallyAdded) ClusterAware.removeCacheManager(cacheManagerName);
      delegate.stop();
   }

   @Override
   public DomainDataRegion buildDomainDataRegion(DomainDataRegionConfig regionConfig, DomainDataRegionBuildingContext buildingContext) {
      return delegate.buildDomainDataRegion(regionConfig, buildingContext);
   }

   @Override
   public QueryResultsRegion buildQueryResultsRegion(String regionName, SessionFactoryImplementor sessionFactory)
         throws CacheException {
      return delegate.buildQueryResultsRegion(regionName, sessionFactory);
   }

   @Override
   public TimestampsRegion buildTimestampsRegion(String regionName, SessionFactoryImplementor sessionFactory)
         throws CacheException {
      return delegate.buildTimestampsRegion(regionName, sessionFactory);
   }

   @Override
   public boolean isMinimalPutsEnabledByDefault() {
      return delegate.isMinimalPutsEnabledByDefault();
   }

   @Override
   public AccessType getDefaultAccessType() {
      return AccessType.TRANSACTIONAL;
   }

   @Override
   public String qualify(String regionName) {
      return delegate.qualify(regionName);
   }

   public long nextTimestamp() {
      return delegate.nextTimestamp();
   }

   @Override
   public CacheTransactionSynchronization createTransactionContext(SharedSessionContractImplementor session) {
      return delegate.createTransactionContext(session);
   }

   @Override
   public long getTimeout() {
      return delegate.getTimeout();
   }
}
