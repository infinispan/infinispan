/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.test.cache.infinispan;
<<<<<<< HEAD
import static org.hibernate.TestLogger.LOG;
=======

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> HHH-5942 - Migrate to JUnit 4
=======
=======
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
>>>>>>> HHH-7898 Regression on org.hibernate.cache.infinispan.query.QueryResultsRegionImpl.put(Object, Object)
import java.util.Properties;
>>>>>>> HHH-9490 - Migrate from dom4j to jaxb for XML processing;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cache.infinispan.InfinispanRegionFactory;
import org.hibernate.cache.infinispan.impl.BaseGeneralDataRegion;
import org.hibernate.cache.infinispan.impl.BaseRegion;
import org.hibernate.cache.spi.GeneralDataRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.Region;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.engine.spi.SharedSessionContractImplementor;

import org.hibernate.test.cache.infinispan.util.CacheTestUtil;
import org.hibernate.test.cache.infinispan.util.ExpectingInterceptor;
import org.hibernate.test.cache.infinispan.util.TestInfinispanRegionFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.junit.Test;

import org.infinispan.AdvancedCache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Base class for tests of QueryResultsRegion and TimestampsRegion.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
<<<<<<< HEAD:src/test/java/org/hibernate/test/cache/infinispan/AbstractGeneralDataRegionTestCase.java
<<<<<<< HEAD
<<<<<<< HEAD
public abstract class AbstractGeneralDataRegionTestCase extends AbstractRegionImplTestCase {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    protected static final String KEY = "Key";

    protected static final String VALUE1 = "value1";
    protected static final String VALUE2 = "value2";

    public AbstractGeneralDataRegionTestCase( String name ) {
        super(name);
    }

    @Override
    protected void putInRegion( Region region,
                                Object key,
                                Object value ) {
        ((GeneralDataRegion)region).put(key, value);
    }

    @Override
    protected void removeFromRegion( Region region,
                                     Object key ) {
        ((GeneralDataRegion)region).evict(key);
    }

    /**
     * Test method for {@link QueryResultsRegion#evict(java.lang.Object)}. FIXME add testing of the
     * "immediately without regard for transaction isolation" bit in the CollectionRegionAccessStrategy API.
     */
    public void testEvict() throws Exception {
        evictOrRemoveTest();
    }

    private void evictOrRemoveTest() throws Exception {
        Configuration cfg = createConfiguration();
        InfinispanRegionFactory regionFactory = CacheTestUtil.startRegionFactory(getJdbcServices(), cfg, getCacheTestSupport());
        CacheAdapter localCache = getInfinispanCache(regionFactory);
        boolean invalidation = localCache.isClusteredInvalidation();

        // Sleep a bit to avoid concurrent FLUSH problem
        avoidConcurrentFlush();

        GeneralDataRegion localRegion = (GeneralDataRegion)createRegion(regionFactory,
                                                                        getStandardRegionName(REGION_PREFIX),
                                                                        cfg.getProperties(),
                                                                        null);

        cfg = createConfiguration();
        regionFactory = CacheTestUtil.startRegionFactory(getJdbcServices(), cfg, getCacheTestSupport());

        GeneralDataRegion remoteRegion = (GeneralDataRegion)createRegion(regionFactory,
                                                                         getStandardRegionName(REGION_PREFIX),
                                                                         cfg.getProperties(),
                                                                         null);

        assertNull("local is clean", localRegion.get(KEY));
        assertNull("remote is clean", remoteRegion.get(KEY));

        localRegion.put(KEY, VALUE1);
        assertEquals(VALUE1, localRegion.get(KEY));

        // allow async propagation
        sleep(250);
        Object expected = invalidation ? null : VALUE1;
        assertEquals(expected, remoteRegion.get(KEY));

        localRegion.evict(KEY);

        // allow async propagation
        sleep(250);
        assertEquals(null, localRegion.get(KEY));
        assertEquals(null, remoteRegion.get(KEY));
    }

    protected abstract String getStandardRegionName( String regionPrefix );

    /**
     * Test method for {@link QueryResultsRegion#evictAll()}. FIXME add testing of the
     * "immediately without regard for transaction isolation" bit in the CollectionRegionAccessStrategy API.
     */
    public void testEvictAll() throws Exception {
        evictOrRemoveAllTest("entity");
    }

    private void evictOrRemoveAllTest( String configName ) throws Exception {
        Configuration cfg = createConfiguration();
        InfinispanRegionFactory regionFactory = CacheTestUtil.startRegionFactory(getJdbcServices(), cfg, getCacheTestSupport());
        CacheAdapter localCache = getInfinispanCache(regionFactory);

        // Sleep a bit to avoid concurrent FLUSH problem
        avoidConcurrentFlush();

        GeneralDataRegion localRegion = (GeneralDataRegion)createRegion(regionFactory,
                                                                        getStandardRegionName(REGION_PREFIX),
                                                                        cfg.getProperties(),
                                                                        null);

        cfg = createConfiguration();
        regionFactory = CacheTestUtil.startRegionFactory(getJdbcServices(), cfg, getCacheTestSupport());
        CacheAdapter remoteCache = getInfinispanCache(regionFactory);

        // Sleep a bit to avoid concurrent FLUSH problem
        avoidConcurrentFlush();

        GeneralDataRegion remoteRegion = (GeneralDataRegion)createRegion(regionFactory,
                                                                         getStandardRegionName(REGION_PREFIX),
                                                                         cfg.getProperties(),
                                                                         null);

        Set keys = localCache.keySet();
        assertEquals("No valid children in " + keys, 0, getValidKeyCount(keys));

        keys = remoteCache.keySet();
        assertEquals("No valid children in " + keys, 0, getValidKeyCount(keys));

        assertNull("local is clean", localRegion.get(KEY));
        assertNull("remote is clean", remoteRegion.get(KEY));

        localRegion.put(KEY, VALUE1);
        assertEquals(VALUE1, localRegion.get(KEY));

        // Allow async propagation
        sleep(250);

        remoteRegion.put(KEY, VALUE1);
        assertEquals(VALUE1, remoteRegion.get(KEY));

        // Allow async propagation
        sleep(250);

        localRegion.evictAll();

        // allow async propagation
        sleep(250);
        // This should re-establish the region root node in the optimistic case
        assertNull(localRegion.get(KEY));
        assertEquals("No valid children in " + keys, 0, getValidKeyCount(localCache.keySet()));

        // Re-establishing the region root on the local node doesn't
        // propagate it to other nodes. Do a get on the remote node to re-establish
        // This only adds a node in the case of optimistic locking
        assertEquals(null, remoteRegion.get(KEY));
        assertEquals("No valid children in " + keys, 0, getValidKeyCount(remoteCache.keySet()));

        assertEquals("local is clean", null, localRegion.get(KEY));
        assertEquals("remote is clean", null, remoteRegion.get(KEY));
    }

    protected Configuration createConfiguration() {
        Configuration cfg = CacheTestUtil.buildConfiguration("test", InfinispanRegionFactory.class, false, true);
        return cfg;
    }

    protected void rollback() {
        try {
            BatchModeTransactionManager.getInstance().rollback();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
=======
   protected static final String KEY = "Key";

   protected static final String VALUE1 = "value1";
   protected static final String VALUE2 = "value2";

   public AbstractGeneralDataRegionTestCase(String name) {
      super(name);
   }

   @Override
   protected void putInRegion(Region region, Object key, Object value) {
      ((GeneralDataRegion) region).put(key, value);
   }

   @Override
   protected void removeFromRegion(Region region, Object key) {
      ((GeneralDataRegion) region).evict(key);
   }

   /**
    * Test method for {@link QueryResultsRegion#evict(java.lang.Object)}.
    * 
    * FIXME add testing of the "immediately without regard for transaction isolation" bit in the
    * CollectionRegionAccessStrategy API.
    */
   public void testEvict() throws Exception {
      evictOrRemoveTest();
   }

   private void evictOrRemoveTest() throws Exception {
      Configuration cfg = createConfiguration();
      InfinispanRegionFactory regionFactory = CacheTestUtil.startRegionFactory(
			  getServiceRegistry( cfg.getProperties() ), cfg, getCacheTestSupport()
	  );
      CacheAdapter localCache = getInfinispanCache(regionFactory);
      boolean invalidation = localCache.isClusteredInvalidation();

      // Sleep a bit to avoid concurrent FLUSH problem
      avoidConcurrentFlush();

      GeneralDataRegion localRegion = (GeneralDataRegion) createRegion(regionFactory,
               getStandardRegionName(REGION_PREFIX), cfg.getProperties(), null);

      cfg = createConfiguration();
      regionFactory = CacheTestUtil.startRegionFactory(
			  getServiceRegistry( cfg.getProperties() ), cfg, getCacheTestSupport()
	  );

      GeneralDataRegion remoteRegion = (GeneralDataRegion) createRegion(regionFactory,
               getStandardRegionName(REGION_PREFIX), cfg.getProperties(), null);

      assertNull("local is clean", localRegion.get(KEY));
      assertNull("remote is clean", remoteRegion.get(KEY));

      localRegion.put(KEY, VALUE1);
      assertEquals(VALUE1, localRegion.get(KEY));

      // allow async propagation
      sleep(250);
      Object expected = invalidation ? null : VALUE1;
      assertEquals(expected, remoteRegion.get(KEY));

      localRegion.evict(KEY);

      // allow async propagation
      sleep(250);
      assertEquals(null, localRegion.get(KEY));
      assertEquals(null, remoteRegion.get(KEY));
   }

   protected abstract String getStandardRegionName(String regionPrefix);

   /**
    * Test method for {@link QueryResultsRegion#evictAll()}.
    * 
    * FIXME add testing of the "immediately without regard for transaction isolation" bit in the
    * CollectionRegionAccessStrategy API.
    */
   public void testEvictAll() throws Exception {
      evictOrRemoveAllTest("entity");
   }

   private void evictOrRemoveAllTest(String configName) throws Exception {
      Configuration cfg = createConfiguration();
      InfinispanRegionFactory regionFactory = CacheTestUtil.startRegionFactory(
			  getServiceRegistry( cfg.getProperties() ), cfg, getCacheTestSupport()
	  );
      CacheAdapter localCache = getInfinispanCache(regionFactory);

      // Sleep a bit to avoid concurrent FLUSH problem
      avoidConcurrentFlush();

      GeneralDataRegion localRegion = (GeneralDataRegion) createRegion(regionFactory,
               getStandardRegionName(REGION_PREFIX), cfg.getProperties(), null);

      cfg = createConfiguration();
      regionFactory = CacheTestUtil.startRegionFactory(
			  getServiceRegistry( cfg.getProperties() ), cfg, getCacheTestSupport()
	  );
      CacheAdapter remoteCache = getInfinispanCache(regionFactory);

      // Sleep a bit to avoid concurrent FLUSH problem
      avoidConcurrentFlush();

      GeneralDataRegion remoteRegion = (GeneralDataRegion) createRegion(regionFactory,
               getStandardRegionName(REGION_PREFIX), cfg.getProperties(), null);

      Set keys = localCache.keySet();
      assertEquals("No valid children in " + keys, 0, getValidKeyCount(keys));

      keys = remoteCache.keySet();
      assertEquals("No valid children in " + keys, 0, getValidKeyCount(keys));

      assertNull("local is clean", localRegion.get(KEY));
      assertNull("remote is clean", remoteRegion.get(KEY));

      localRegion.put(KEY, VALUE1);
      assertEquals(VALUE1, localRegion.get(KEY));

      // Allow async propagation
      sleep(250);

      remoteRegion.put(KEY, VALUE1);
      assertEquals(VALUE1, remoteRegion.get(KEY));

      // Allow async propagation
      sleep(250);

      localRegion.evictAll();

      // allow async propagation
      sleep(250);
      // This should re-establish the region root node in the optimistic case
      assertNull(localRegion.get(KEY));
      assertEquals("No valid children in " + keys, 0, getValidKeyCount(localCache.keySet()));

      // Re-establishing the region root on the local node doesn't
      // propagate it to other nodes. Do a get on the remote node to re-establish
      // This only adds a node in the case of optimistic locking
      assertEquals(null, remoteRegion.get(KEY));
      assertEquals("No valid children in " + keys, 0, getValidKeyCount(remoteCache.keySet()));

      assertEquals("local is clean", null, localRegion.get(KEY));
      assertEquals("remote is clean", null, remoteRegion.get(KEY));
   }

   protected Configuration createConfiguration() {
      Configuration cfg = CacheTestUtil.buildConfiguration("test", InfinispanRegionFactory.class, false, true);
      return cfg;
   }

   protected void rollback() {
      try {
         BatchModeTransactionManager.getInstance().rollback();
      } catch (Exception e) {
         log.error(e.getMessage(), e);
      }
   }
}
>>>>>>> HHH-5949 - Migrate, complete and integrate TransactionFactory as a service
=======
=======
=======
public abstract class AbstractGeneralDataRegionTestCase<T extends CacheKey> extends AbstractRegionImplTestCase<T> {
>>>>>>> HHH-9840 Allow 2nd level cache implementations to customize the various key implementations
=======
public abstract class AbstractGeneralDataRegionTestCase extends AbstractRegionImplTestCase {
>>>>>>> HHH-9840 Change all kinds of CacheKey contract to a raw Object
	private static final Logger log = Logger.getLogger( AbstractGeneralDataRegionTestCase.class );
=======
public abstract class AbstractGeneralDataRegionTest extends AbstractRegionImplTest {
<<<<<<< HEAD
	private static final Logger log = Logger.getLogger( AbstractGeneralDataRegionTest.class );
>>>>>>> HHH-10030 Add read-write cache concurrency strategy to Infinispan 2LC:src/test/java/org/hibernate/test/cache/infinispan/AbstractGeneralDataRegionTest.java

>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
=======
>>>>>>> HHH-11344 Testsuite speed-up
	protected static final String KEY = "Key";

	protected static final String VALUE1 = "value1";
	protected static final String VALUE2 = "value2";
	protected static final String VALUE3 = "value3";

	@Override
	public List<Object[]> getCacheModeParameters() {
		// the actual cache mode and access type is irrelevant for the general data regions
		return Arrays.<Object[]>asList(new Object[]{ CacheMode.INVALIDATION_SYNC, AccessType.TRANSACTIONAL });
	}

	@Override
	protected void putInRegion(Region region, Object key, Object value) {
		((GeneralDataRegion) region).put(null, key, value );
	}

	@Override
	protected void removeFromRegion(Region region, Object key) {
		((GeneralDataRegion) region).evict( key );
	}

	protected interface SFRConsumer {
		void accept(List<SessionFactory> sessionFactories, List<GeneralDataRegion> regions) throws Exception;
	}

	protected void withSessionFactoriesAndRegions(int num, SFRConsumer consumer) throws Exception {
		StandardServiceRegistryBuilder ssrb = createStandardServiceRegistryBuilder()
				.applySetting(AvailableSettings.CACHE_REGION_FACTORY, TestInfinispanRegionFactory.class.getName());
		Properties properties = CacheTestUtil.toProperties( ssrb.getSettings() );
		List<StandardServiceRegistry> registries = new ArrayList<>();
		List<SessionFactory> sessionFactories = new ArrayList<>();
		List<GeneralDataRegion> regions = new ArrayList<>();
		for (int i = 0; i < num; ++i) {
			StandardServiceRegistry registry = ssrb.build();
			registries.add(registry);

			SessionFactory sessionFactory = new MetadataSources(registry).buildMetadata().buildSessionFactory();
			sessionFactories.add(sessionFactory);

			InfinispanRegionFactory regionFactory = (InfinispanRegionFactory) registry.getService(RegionFactory.class);
			GeneralDataRegion region = (GeneralDataRegion) createRegion(
					regionFactory,
					getStandardRegionName( REGION_PREFIX ),
					properties,
					null
			);
			regions.add(region);
		}
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

	@Test
	public void testEvict() throws Exception {
		withSessionFactoriesAndRegions(2, ((sessionFactories, regions) -> {
			GeneralDataRegion localRegion = regions.get(0);
			GeneralDataRegion remoteRegion = regions.get(1);
			SharedSessionContractImplementor localSession = (SharedSessionContractImplementor) sessionFactories.get(0).openSession();
			SharedSessionContractImplementor remoteSession = (SharedSessionContractImplementor) sessionFactories.get(1).openSession();
			AdvancedCache localCache = ((BaseRegion) localRegion).getCache();
			AdvancedCache remoteCache = ((BaseRegion) remoteRegion).getCache();
			try {
				assertNull("local is clean", localRegion.get(localSession, KEY));
				assertNull("remote is clean", remoteRegion.get(remoteSession, KEY));

				// If this node is backup owner, it will see the update once as originator and then when getting the value from primary
				boolean isLocalNodeBackupOwner = localCache.getDistributionManager().locate(KEY).indexOf(localCache.getCacheManager().getAddress()) > 0;
				CountDownLatch insertLatch = new CountDownLatch(isLocalNodeBackupOwner ? 3 : 2);
				ExpectingInterceptor.get(localCache).when((ctx, cmd) -> cmd instanceof PutKeyValueCommand).countDown(insertLatch);
				ExpectingInterceptor.get(remoteCache).when((ctx, cmd) -> cmd instanceof PutKeyValueCommand).countDown(insertLatch);

				Transaction tx = localSession.getTransaction();
				tx.begin();
				try {
					localRegion.put(localSession, KEY, VALUE1);
					tx.commit();
				} catch (Exception e) {
					tx.rollback();
					throw e;
				}

				assertTrue(insertLatch.await(2, TimeUnit.SECONDS));
				assertEquals(VALUE1, localRegion.get(localSession, KEY));
				assertEquals(VALUE1, remoteRegion.get(remoteSession, KEY));

				CountDownLatch removeLatch = new CountDownLatch(isLocalNodeBackupOwner ? 3 : 2);
				ExpectingInterceptor.get(localCache).when((ctx, cmd) -> cmd instanceof RemoveCommand).countDown(removeLatch);
				ExpectingInterceptor.get(remoteCache).when((ctx, cmd) -> cmd instanceof RemoveCommand).countDown(removeLatch);

				regionEvict(localRegion);

				assertTrue(removeLatch.await(2, TimeUnit.SECONDS));
				assertEquals(null, localRegion.get(localSession, KEY));
				assertEquals(null, remoteRegion.get(remoteSession, KEY));
			} finally {
				localSession.close();
				remoteSession.close();

				ExpectingInterceptor.cleanup(localCache, remoteCache);
			}
		}));
	}

	protected void regionEvict(GeneralDataRegion region) throws Exception {
	  region.evict(KEY);
	}

	protected abstract String getStandardRegionName(String regionPrefix);

	/**
	 * Test method for {@link QueryResultsRegion#evictAll()}.
	 * <p/>
	 * FIXME add testing of the "immediately without regard for transaction isolation" bit in the
	 * CollectionRegionAccessStrategy API.
	 */
	public void testEvictAll() throws Exception {
		withSessionFactoriesAndRegions(2, (sessionFactories, regions) -> {
			GeneralDataRegion localRegion = regions.get(0);
			GeneralDataRegion remoteRegion = regions.get(1);
			AdvancedCache localCache = ((BaseGeneralDataRegion) localRegion).getCache();
			AdvancedCache remoteCache = ((BaseGeneralDataRegion) remoteRegion).getCache();
			SharedSessionContractImplementor localSession = (SharedSessionContractImplementor) sessionFactories.get(0).openSession();
			SharedSessionContractImplementor remoteSession = (SharedSessionContractImplementor) sessionFactories.get(1).openSession();

			try {
				Set localKeys = localCache.keySet();
				assertEquals( "No valid children in " + localKeys, 0, localKeys.size() );

				Set remoteKeys = remoteCache.keySet();
				assertEquals( "No valid children in " + remoteKeys, 0, remoteKeys.size() );

				assertNull( "local is clean", localRegion.get(null, KEY ) );
				assertNull( "remote is clean", remoteRegion.get(null, KEY ) );

				localRegion.put(localSession, KEY, VALUE1);
				assertEquals( VALUE1, localRegion.get(null, KEY ) );

				remoteRegion.put(remoteSession, KEY, VALUE1);
				assertEquals( VALUE1, remoteRegion.get(null, KEY ) );

				localRegion.evictAll();

				// This should re-establish the region root node in the optimistic case
				assertNull( localRegion.get(null, KEY ) );
				localKeys = localCache.keySet();
				assertEquals( "No valid children in " + localKeys, 0, localKeys.size() );

				// Re-establishing the region root on the local node doesn't
				// propagate it to other nodes. Do a get on the remote node to re-establish
				// This only adds a node in the case of optimistic locking
				assertEquals( null, remoteRegion.get(null, KEY ) );
				remoteKeys = remoteCache.keySet();
				assertEquals( "No valid children in " + remoteKeys, 0, remoteKeys.size() );

				assertEquals( "local is clean", null, localRegion.get(null, KEY ) );
				assertEquals( "remote is clean", null, remoteRegion.get(null, KEY ) );
			} finally {
				localSession.close();
				remoteSession.close();
			}

		});
	}
}
<<<<<<< HEAD
>>>>>>> HHH-5942 - Migrate to JUnit 4
=======
>>>>>>> HHH-7898 Regression on org.hibernate.cache.infinispan.query.QueryResultsRegionImpl.put(Object, Object)
