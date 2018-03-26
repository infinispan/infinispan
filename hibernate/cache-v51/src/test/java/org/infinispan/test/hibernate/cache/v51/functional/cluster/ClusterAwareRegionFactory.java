/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.v51.functional.cluster;

import java.util.Properties;

import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.cache.spi.access.AccessType;

import org.infinispan.hibernate.cache.v51.InfinispanRegionFactory;
import org.infinispan.test.hibernate.cache.commons.functional.cluster.ClusterAware;
import org.infinispan.test.hibernate.cache.commons.functional.cluster.DualNodeTest;
import org.infinispan.test.hibernate.cache.commons.util.CacheTestUtil;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * ClusterAwareRegionFactory.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class ClusterAwareRegionFactory implements RegionFactory {

	private InfinispanRegionFactory delegate;
	private String cacheManagerName;
	private boolean locallyAdded;

	public ClusterAwareRegionFactory(Properties props) {
		Class<? extends InfinispanRegionFactory> regionFactoryClass =
				(Class<InfinispanRegionFactory>) props.get(DualNodeTest.REGION_FACTORY_DELEGATE);
		delegate = CacheTestUtil.createRegionFactory(regionFactoryClass, props);
	}

	public void start(SessionFactoryOptions settings, Properties properties) throws CacheException {
		cacheManagerName = properties.getProperty(DualNodeTest.NODE_ID_PROP);

		EmbeddedCacheManager existing = ClusterAware.getCacheManager(cacheManagerName);
		locallyAdded = (existing == null);

		if (locallyAdded) {
			delegate.start(settings, properties);
			ClusterAware.addCacheManager(cacheManagerName, delegate.getCacheManager());
		} else {
			delegate.setCacheManager(existing);
		}
	}

	public void stop() {
		if (locallyAdded) ClusterAware.removeCacheManager(cacheManagerName);
		delegate.stop();
	}

	public CollectionRegion buildCollectionRegion(String regionName, Properties properties,
				CacheDataDescription metadata) throws CacheException {
		return delegate.buildCollectionRegion(regionName, properties, metadata);
	}

	public EntityRegion buildEntityRegion(String regionName, Properties properties,
				CacheDataDescription metadata) throws CacheException {
		return delegate.buildEntityRegion(regionName, properties, metadata);
	}

	public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties properties, CacheDataDescription metadata)
			throws CacheException {
		return delegate.buildNaturalIdRegion( regionName, properties, metadata );
	}

	public QueryResultsRegion buildQueryResultsRegion(String regionName, Properties properties)
				throws CacheException {
		return delegate.buildQueryResultsRegion(regionName, properties);
	}

	public TimestampsRegion buildTimestampsRegion(String regionName, Properties properties)
				throws CacheException {
		return delegate.buildTimestampsRegion(regionName, properties);
	}

	@Override
	public boolean isMinimalPutsEnabledByDefault() {
		return delegate.isMinimalPutsEnabledByDefault();
	}

	@Override
	public AccessType getDefaultAccessType() {
		return AccessType.TRANSACTIONAL;
	}

	public long nextTimestamp() {
		return delegate.nextTimestamp();
	}
}
