/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.timestamp;

import java.util.Properties;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.infinispan.hibernate.cache.InfinispanRegionFactory;
import org.infinispan.hibernate.cache.timestamp.TimestampsRegionImpl;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.Region;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.UpdateTimestampsCache;

import org.infinispan.test.hibernate.cache.AbstractGeneralDataRegionTest;
import org.infinispan.test.hibernate.cache.functional.entities.Account;
import org.infinispan.test.hibernate.cache.functional.entities.AccountHolder;
import org.infinispan.test.hibernate.cache.functional.classloader.SelectedClassnameClassLoader;
import org.infinispan.test.hibernate.cache.util.CacheTestUtil;
import org.infinispan.test.hibernate.cache.util.ClassLoaderAwareCache;

import org.infinispan.test.hibernate.cache.util.TestInfinispanRegionFactory;
import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.Event;
import org.junit.Test;

/**
 * Tests of TimestampsRegionImpl.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class TimestampsRegionImplTest extends AbstractGeneralDataRegionTest {

	 @Override
	protected String getStandardRegionName(String regionPrefix) {
		return regionPrefix + "/" + UpdateTimestampsCache.class.getName();
	}

	@Override
	protected Region createRegion(InfinispanRegionFactory regionFactory, String regionName, Properties properties, CacheDataDescription cdd) {
		return regionFactory.buildTimestampsRegion(regionName, properties);
	}

	@Override
	protected AdvancedCache getInfinispanCache(InfinispanRegionFactory regionFactory) {
		return regionFactory.getCacheManager().getCache("timestamps").getAdvancedCache();
	}

   @Test
	public void testClearTimestampsRegionInIsolated() throws Exception {
		StandardServiceRegistryBuilder ssrb = createStandardServiceRegistryBuilder();
		final StandardServiceRegistry registry = ssrb.build();
		final StandardServiceRegistry registry2 = ssrb.build();

		try {
			final Properties properties = CacheTestUtil.toProperties( ssrb.getSettings() );

			InfinispanRegionFactory regionFactory = CacheTestUtil.startRegionFactory(
					  registry,
					  getCacheTestSupport()
			);

			InfinispanRegionFactory regionFactory2 = CacheTestUtil.startRegionFactory(
					  registry2,
					  getCacheTestSupport()
			);

			TimestampsRegionImpl region = (TimestampsRegionImpl) regionFactory.buildTimestampsRegion(
					  getStandardRegionName(REGION_PREFIX),
					  properties
			);
			TimestampsRegionImpl region2 = (TimestampsRegionImpl) regionFactory2.buildTimestampsRegion(
					  getStandardRegionName(REGION_PREFIX),
					  properties
			);

			Account acct = new Account();
			acct.setAccountHolder(new AccountHolder());
			region.getCache().withFlags(Flag.FORCE_SYNCHRONOUS).put(acct, "boo");
		}
		finally {
			StandardServiceRegistryBuilder.destroy( registry );
			StandardServiceRegistryBuilder.destroy( registry2 );
		}
	}

	@Override
	protected Class<? extends RegionFactory> getRegionFactoryClass() {
		return MockInfinispanRegionFactory.class;
	}

	public static class MockInfinispanRegionFactory extends TestInfinispanRegionFactory {

		public MockInfinispanRegionFactory(Properties properties) {
			super(properties);
		}

		@Override
		protected AdvancedCache createCacheWrapper(AdvancedCache cache) {
			return new ClassLoaderAwareCache(cache, Thread.currentThread().getContextClassLoader()) {
				@Override
				public void addListener(Object listener) {
					super.addListener(new MockClassLoaderAwareListener(listener, this));
				}
			};
		}

		@Listener
		public static class MockClassLoaderAwareListener extends ClassLoaderAwareCache.ClassLoaderAwareListener {
			MockClassLoaderAwareListener(Object listener, ClassLoaderAwareCache cache) {
				super(listener, cache);
			}

			@CacheEntryActivated
			@CacheEntryCreated
			@CacheEntryInvalidated
			@CacheEntryLoaded
			@CacheEntryModified
			@CacheEntryPassivated
			@CacheEntryRemoved
			@CacheEntryVisited
			public void event(Event event) throws Throwable {
				ClassLoader cl = Thread.currentThread().getContextClassLoader();
				String notFoundPackage = "org.infinispan.test.hibernate.cache.functional.entities";
				String[] notFoundClasses = { notFoundPackage + ".Account", notFoundPackage + ".AccountHolder" };
				SelectedClassnameClassLoader visible = new SelectedClassnameClassLoader(null, null, notFoundClasses, cl);
				Thread.currentThread().setContextClassLoader(visible);
				super.event(event);
				Thread.currentThread().setContextClassLoader(cl);
			}
		}
	}
}
