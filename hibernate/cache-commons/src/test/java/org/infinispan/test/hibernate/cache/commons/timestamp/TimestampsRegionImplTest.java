/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.timestamp;

import java.util.Properties;
import java.util.function.Function;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.infinispan.commons.test.categories.Smoke;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;

import org.infinispan.test.hibernate.cache.commons.AbstractGeneralDataRegionTest;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Account;
import org.infinispan.test.hibernate.cache.commons.functional.entities.AccountHolder;
import org.infinispan.test.hibernate.cache.commons.functional.classloader.SelectedClassnameClassLoader;
import org.infinispan.test.hibernate.cache.commons.util.CacheTestUtil;
import org.infinispan.test.hibernate.cache.commons.util.ClassLoaderAwareCache;

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
import org.junit.experimental.categories.Category;

/**
 * Tests of TimestampsRegionImpl.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
@Category(Smoke.class)
public class TimestampsRegionImplTest extends AbstractGeneralDataRegionTest {

	@Override
	protected InfinispanBaseRegion createRegion(TestRegionFactory regionFactory, String regionName) {
		return regionFactory.buildTimestampsRegion(regionName);
	}

	@Test
	public void testClearTimestampsRegionInIsolated() throws Exception {
		StandardServiceRegistryBuilder ssrb = createStandardServiceRegistryBuilder();
		final StandardServiceRegistry registry = ssrb.build();
		final StandardServiceRegistry registry2 = ssrb.build();

		try {
			final Properties properties = CacheTestUtil.toProperties( ssrb.getSettings() );

			TestRegionFactory regionFactory = CacheTestUtil.startRegionFactory(
					  registry,
					  getCacheTestSupport()
			);

			TestRegionFactory regionFactory2 = CacheTestUtil.startRegionFactory(
					  registry2,
					  getCacheTestSupport()
			);

			InfinispanBaseRegion region = regionFactory.buildTimestampsRegion(REGION_PREFIX + "/timestamps");
			InfinispanBaseRegion region2 = regionFactory2.buildTimestampsRegion(REGION_PREFIX + "/timestamps");

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
	protected StandardServiceRegistryBuilder createStandardServiceRegistryBuilder() {
		StandardServiceRegistryBuilder ssrb = super.createStandardServiceRegistryBuilder();
		ssrb.applySetting(TestRegionFactory.WRAP_CACHE, (Function<AdvancedCache, AdvancedCache>) cache ->
         new ClassLoaderAwareCache(cache, Thread.currentThread().getContextClassLoader()) {
            @Override
            public void addListener(Object listener) {
               super.addListener(new MockClassLoaderAwareListener(listener, this));
            }
         }
		);
		return ssrb;
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
