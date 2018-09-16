/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.SharedSessionContract;
import org.hibernate.Transaction;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.engine.transaction.jta.platform.internal.NoJtaPlatform;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.AvailableSettings;

import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.hibernate.cache.commons.util.BatchModeJtaPlatform;
import org.infinispan.test.hibernate.cache.commons.util.CacheTestUtil;
import org.infinispan.test.hibernate.cache.commons.util.ExpectingInterceptor;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess.TestRegion;
import org.junit.Test;

import org.infinispan.AdvancedCache;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Base class for tests of QueryResultsRegion and TimestampsRegion.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public abstract class AbstractGeneralDataRegionTest extends AbstractRegionImplTest {
	protected static final String KEY = "Key";

	protected static final String VALUE1 = "value1";
	protected static final String VALUE2 = "value2";
	protected static final String VALUE3 = "value3";

   protected static final TestSessionAccess TEST_SESSION_ACCESS = TestSessionAccess.findTestSessionAccess();

	@Override
	public List<Object[]> getParameters() {
		// the actual cache mode and access type is irrelevant for the general data regions
		return Arrays.asList(
				new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.INVALIDATION_SYNC, AccessType.TRANSACTIONAL},
				new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.INVALIDATION_SYNC, AccessType.READ_WRITE}
		);
	}

	protected interface SFRConsumer {
		void accept(List<SessionFactory> sessionFactories, List<InfinispanBaseRegion> regions) throws Exception;
	}

	protected void withSessionFactoriesAndRegions(int num, SFRConsumer consumer) throws Exception {
		StandardServiceRegistryBuilder ssrb = createStandardServiceRegistryBuilder()
				.applySetting(AvailableSettings.CACHE_REGION_FACTORY, TestRegionFactoryProvider.load().getRegionFactoryClass().getName());
		Properties properties = CacheTestUtil.toProperties( ssrb.getSettings() );
		List<StandardServiceRegistry> registries = new ArrayList<>();
		List<SessionFactory> sessionFactories = new ArrayList<>();
		List<InfinispanBaseRegion> regions = new ArrayList<>();
		for (int i = 0; i < num; ++i) {
			StandardServiceRegistry registry = ssrb.build();
			registries.add(registry);

			SessionFactory sessionFactory = new MetadataSources(registry).buildMetadata().buildSessionFactory();
			sessionFactories.add(sessionFactory);

			TestRegionFactory regionFactory = TestRegionFactoryProvider.load().wrap(registry.getService(RegionFactory.class));
			InfinispanBaseRegion region = createRegion(regionFactory, REGION_PREFIX + "/who-cares");
			regions.add(region);
		}
      waitForClusterToForm(regions);
		try {
			consumer.accept(sessionFactories, regions);
		} finally {
			for (SessionFactory sessionFactory : sessionFactories) {
				sessionFactory.close();
			}
			for (StandardServiceRegistry registry : registries) {
				StandardServiceRegistryBuilder.destroy( registry );
			}
		}
	}

   private void waitForClusterToForm(List<InfinispanBaseRegion> regions) {
      List<AdvancedCache> caches = regions.stream()
            .map(InfinispanBaseRegion::getCache)
            .collect(Collectors.toList());

      TestingUtil.blockUntilViewsReceived(20000, caches);
      TestingUtil.waitForNoRebalance(caches);
   }

	@Test
   @Category(TestDisabledIn53.class) // per-key eviction is not supported in 5.3
	public void testEvict() throws Exception {
		withSessionFactoriesAndRegions(2, ((sessionFactories, regions) -> {
			InfinispanBaseRegion localRegion = regions.get(0);
         TestRegion testLocalRegion = TEST_SESSION_ACCESS.fromRegion(localRegion);
			InfinispanBaseRegion remoteRegion = regions.get(1);
         TestRegion testRemoteRegion = TEST_SESSION_ACCESS.fromRegion(remoteRegion);
         Object localSession = sessionFactories.get(0).openSession();
         Object remoteSession = sessionFactories.get(1).openSession();
			AdvancedCache localCache = localRegion.getCache();
			AdvancedCache remoteCache = remoteRegion.getCache();
			try {
				assertNull("local is clean", testLocalRegion.get(localSession, KEY));
				assertNull("remote is clean", testRemoteRegion.get(remoteSession, KEY));

				// If this node is backup owner, it will see the update once as originator and then when getting the value from primary
				boolean isLocalNodeBackupOwner = localCache.getDistributionManager().locate(KEY).indexOf(localCache.getCacheManager().getAddress()) > 0;
				CountDownLatch insertLatch = new CountDownLatch(isLocalNodeBackupOwner ? 3 : 2);
				ExpectingInterceptor.get(localCache).when((ctx, cmd) -> cmd instanceof PutKeyValueCommand).countDown(insertLatch);
				ExpectingInterceptor.get(remoteCache).when((ctx, cmd) -> cmd instanceof PutKeyValueCommand).countDown(insertLatch);

				Transaction tx = ( (SharedSessionContract) localSession ).getTransaction();
				tx.begin();
				try {
               testLocalRegion.put(localSession, KEY, VALUE1);
					tx.commit();
				} catch (Exception e) {
					tx.rollback();
					throw e;
				}

				assertTrue(insertLatch.await(2, TimeUnit.SECONDS));
				assertEquals(VALUE1, testLocalRegion.get(localSession, KEY));
				assertEquals(VALUE1, testRemoteRegion.get(remoteSession, KEY));

				CountDownLatch removeLatch = new CountDownLatch(isLocalNodeBackupOwner ? 3 : 2);
				ExpectingInterceptor.get(localCache).when((ctx, cmd) -> cmd instanceof RemoveCommand).countDown(removeLatch);
				ExpectingInterceptor.get(remoteCache).when((ctx, cmd) -> cmd instanceof RemoveCommand).countDown(removeLatch);

				regionEvict(localRegion);

				assertTrue(removeLatch.await(2, TimeUnit.SECONDS));
				assertEquals(null, testLocalRegion.get(localSession, KEY));
				assertEquals(null, testRemoteRegion.get(remoteSession, KEY));
			} finally {
				( (Session) localSession ).close();
				( (Session) remoteSession ).close();

				ExpectingInterceptor.cleanup(localCache, remoteCache);
			}
		}));
	}

	protected void regionEvict(InfinispanBaseRegion region) {
	  TEST_SESSION_ACCESS.fromRegion(region).evict(KEY);
	}

	/**
	 * Test method for {@link QueryResultsRegion#evictAll()}.
	 * <p>
	 * FIXME add testing of the "immediately without regard for transaction isolation" bit in the
	 * CollectionRegionAccessStrategy API.
	 */
	public void testEvictAll() throws Exception {
		withSessionFactoriesAndRegions(2, (sessionFactories, regions) -> {
			InfinispanBaseRegion localRegion = regions.get(0);
         TestRegion testLocalRegion = TEST_SESSION_ACCESS.fromRegion(localRegion);
			InfinispanBaseRegion remoteRegion = regions.get(1);
         TestRegion testRemoteRegion = TEST_SESSION_ACCESS.fromRegion(remoteRegion);
			AdvancedCache localCache = localRegion.getCache();
			AdvancedCache remoteCache = remoteRegion.getCache();
			Object localSession = sessionFactories.get(0).openSession();
         Object remoteSession = sessionFactories.get(1).openSession();

			try {
				Set localKeys = localCache.keySet();
				assertEquals( "No valid children in " + localKeys, 0, localKeys.size() );

				Set remoteKeys = remoteCache.keySet();
				assertEquals( "No valid children in " + remoteKeys, 0, remoteKeys.size() );

				assertNull( "local is clean", testLocalRegion.get(null, KEY ) );
				assertNull( "remote is clean", testRemoteRegion.get(null, KEY ) );

				testLocalRegion.put(localSession, KEY, VALUE1);
				assertEquals( VALUE1, testLocalRegion.get(null, KEY ) );

				testRemoteRegion.put(remoteSession, KEY, VALUE1);
				assertEquals( VALUE1, testRemoteRegion.get(null, KEY ) );

				testLocalRegion.evictAll();

				// This should re-establish the region root node in the optimistic case
				assertNull( testLocalRegion.get(null, KEY ) );
				localKeys = localCache.keySet();
				assertEquals( "No valid children in " + localKeys, 0, localKeys.size() );

				// Re-establishing the region root on the local node doesn't
				// propagate it to other nodes. Do a get on the remote node to re-establish
				// This only adds a node in the case of optimistic locking
				assertEquals( null, testRemoteRegion.get(null, KEY ) );
				remoteKeys = remoteCache.keySet();
				assertEquals( "No valid children in " + remoteKeys, 0, remoteKeys.size() );

				assertEquals( "local is clean", null, testLocalRegion.get(null, KEY ) );
				assertEquals( "remote is clean", null, testRemoteRegion.get(null, KEY ) );
			} finally {
				( (Session) localSession ).close();
				( (Session) remoteSession ).close();
			}

		});
	}
}
