/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.test.cache.infinispan.collection;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======

<<<<<<< HEAD
>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder
import static org.hibernate.TestLogger.LOG;
=======
import javax.transaction.TransactionManager;
>>>>>>> HHH-5942 - Migrate to JUnit 4
=======

<<<<<<< HEAD
>>>>>>> HHH-7197 reimport imports
=======
import javax.transaction.TransactionManager;
>>>>>>> HHH-9840 Change all kinds of CacheKey contract to a raw Object
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======

<<<<<<< HEAD
>>>>>>> HHH-9840 Allow 2nd level cache implementations to customize the various key implementations
import javax.transaction.TransactionManager;
=======

>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder
import junit.extensions.TestSetup;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestSuite;
<<<<<<< HEAD
=======
import org.infinispan.transaction.tm.BatchModeTransactionManager;

>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder
=======
=======
import javax.transaction.TransactionManager;
>>>>>>> HHH-7197 reimport imports

<<<<<<< HEAD
import junit.framework.AssertionFailedError;
import org.hibernate.cache.infinispan.util.Caches;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.tm.BatchModeTransactionManager;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> HHH-5942 - Migrate to JUnit 4
import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cache.access.CollectionRegionAccessStrategy;
import org.hibernate.cache.impl.CacheDataDescriptionImpl;
=======
import org.hibernate.cache.internal.CacheDataDescriptionImpl;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionRegionAccessStrategy;
>>>>>>> HHH-6191 - repackage org.hibernate.cache per api/spi/internal split
=======
>>>>>>> HHH-7197 reimport imports
=======
=======
import junit.framework.AssertionFailedError;
>>>>>>> HHH-9840 Change all kinds of CacheKey contract to a raw Object
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
>>>>>>> HHH-9490 - Migrate from dom4j to jaxb for XML processing;
import org.hibernate.cache.infinispan.InfinispanRegionFactory;
import org.hibernate.cache.infinispan.access.PutFromLoadValidator;
import org.hibernate.cache.infinispan.access.TransactionalAccessDelegate;
import org.hibernate.cache.infinispan.collection.CollectionRegionImpl;
import org.hibernate.cache.infinispan.util.Caches;
import org.hibernate.cache.internal.CacheDataDescriptionImpl;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionRegionAccessStrategy;
import org.hibernate.internal.util.compare.ComparableComparator;
import org.hibernate.test.cache.infinispan.AbstractNonFunctionalTestCase;
import org.hibernate.test.cache.infinispan.NodeEnvironment;
import org.hibernate.test.cache.infinispan.util.CacheTestUtil;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import org.hibernate.testing.ServiceRegistryBuilder;
<<<<<<< HEAD
import org.hibernate.util.ComparableComparator;
<<<<<<< HEAD
import org.infinispan.transaction.tm.BatchModeTransactionManager;
=======
=======
=======
import org.hibernate.test.cache.infinispan.util.TestingKeyFactory;
<<<<<<< HEAD
>>>>>>> HHH-9840 Allow 2nd level cache implementations to customize the various key implementations
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.AssertionFailedError;

=======
>>>>>>> HHH-9840 Change all kinds of CacheKey contract to a raw Object
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.tm.BatchModeTransactionManager;
import org.jboss.logging.Logger;
<<<<<<< HEAD
>>>>>>> HHH-9490 - Migrate from dom4j to jaxb for XML processing;
=======
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
>>>>>>> HHH-9840 Change all kinds of CacheKey contract to a raw Object

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
>>>>>>> HHH-5942 - Migrate to JUnit 4

=======
=======
>>>>>>> HHH-5986 - Refactor org.hibernate.util package for spi/internal split

import javax.transaction.TransactionManager;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder
/**
 * Base class for tests of CollectionRegionAccessStrategy impls.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public abstract class AbstractCollectionRegionAccessStrategyTestCase extends AbstractNonFunctionalTestCase {
	private static final Logger log = Logger.getLogger( AbstractCollectionRegionAccessStrategyTestCase.class );
<<<<<<< HEAD

<<<<<<< HEAD
    public static final String REGION_NAME = "test/com.foo.test";
    public static final String KEY_BASE = "KEY";
    public static final String VALUE1 = "VALUE1";
    public static final String VALUE2 = "VALUE2";

    protected static int testCount;

    protected static Configuration localCfg;
    protected static InfinispanRegionFactory localRegionFactory;
    protected CacheAdapter localCache;
    protected static Configuration remoteCfg;
    protected static InfinispanRegionFactory remoteRegionFactory;
    protected CacheAdapter remoteCache;

    protected CollectionRegion localCollectionRegion;
    protected CollectionRegionAccessStrategy localAccessStrategy;

    protected CollectionRegion remoteCollectionRegion;
    protected CollectionRegionAccessStrategy remoteAccessStrategy;

    protected boolean invalidation;
    protected boolean synchronous;

    protected Exception node1Exception;
    protected Exception node2Exception;

    protected AssertionFailedError node1Failure;
    protected AssertionFailedError node2Failure;

    public static Test getTestSetup( Class testClass,
                                     String configName ) {
        TestSuite suite = new TestSuite(testClass);
        return new AccessStrategyTestSetup(suite, configName);
    }

    public static Test getTestSetup( Test test,
                                     String configName ) {
        return new AccessStrategyTestSetup(test, configName);
    }

    /**
     * Create a new TransactionalAccessTestCase.
     *
     * @param name
     */
    public AbstractCollectionRegionAccessStrategyTestCase( String name ) {
        super(name);
    }

    protected abstract AccessType getAccessType();

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        // Sleep a bit to avoid concurrent FLUSH problem
        avoidConcurrentFlush();

        localCollectionRegion = localRegionFactory.buildCollectionRegion(REGION_NAME,
                                                                         localCfg.getProperties(),
                                                                         getCacheDataDescription());
        localCache = ((BaseRegion)localCollectionRegion).getCacheAdapter();
        localAccessStrategy = localCollectionRegion.buildAccessStrategy(getAccessType());
        invalidation = localCache.isClusteredInvalidation();
        synchronous = localCache.isSynchronous();

        // Sleep a bit to avoid concurrent FLUSH problem
        avoidConcurrentFlush();

        remoteCollectionRegion = remoteRegionFactory.buildCollectionRegion(REGION_NAME,
                                                                           remoteCfg.getProperties(),
                                                                           getCacheDataDescription());
        remoteCache = ((BaseRegion)remoteCollectionRegion).getCacheAdapter();
        remoteAccessStrategy = remoteCollectionRegion.buildAccessStrategy(getAccessType());

        node1Exception = null;
        node2Exception = null;

        node1Failure = null;
        node2Failure = null;
    }

    @Override
    protected void tearDown() throws Exception {

        super.tearDown();

        try {
            localCache.withFlags(FlagAdapter.CACHE_MODE_LOCAL).clear();
        } catch (Exception e) {
            LOG.error("Problem purging local cache", e);
        }

        try {
            remoteCache.withFlags(FlagAdapter.CACHE_MODE_LOCAL).clear();
        } catch (Exception e) {
            LOG.error("Problem purging remote cache", e);
        }

        node1Exception = null;
        node2Exception = null;

        node1Failure = null;
        node2Failure = null;
    }

    protected static Configuration createConfiguration( String configName,
                                                        String configResource ) {
        Configuration cfg = CacheTestUtil.buildConfiguration(REGION_PREFIX, InfinispanRegionFactory.class, true, false);
        cfg.setProperty(InfinispanRegionFactory.ENTITY_CACHE_RESOURCE_PROP, configName);
        return cfg;
    }

    protected CacheDataDescription getCacheDataDescription() {
        return new CacheDataDescriptionImpl(true, true, ComparableComparator.INSTANCE);
    }

    protected boolean isUsingInvalidation() {
        return invalidation;
    }

    protected boolean isSynchronous() {
        return synchronous;
    }

    /**
     * This is just a setup test where we assert that the cache config is as we expected.
     */
    public abstract void testCacheConfiguration();

    /**
     * Test method for {@link CollectionRegionAccessStrategy#getRegion()}.
     */
    public void testGetRegion() {
        assertEquals("Correct region", localCollectionRegion, localAccessStrategy.getRegion());
    }

    public void testPutFromLoadRemoveDoesNotProduceStaleData() throws Exception {
        final CountDownLatch pferLatch = new CountDownLatch(1);
        final CountDownLatch removeLatch = new CountDownLatch(1);
        TransactionManager tm = DualNodeJtaTransactionManagerImpl.getInstance("test1234");
        PutFromLoadValidator validator = new PutFromLoadValidator(tm) {
            @Override
            public boolean acquirePutFromLoadLock( Object key ) {
                boolean acquired = super.acquirePutFromLoadLock(key);
                try {
                    removeLatch.countDown();
                    pferLatch.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted");
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOG.error("Error", e);
                    throw new RuntimeException("Error", e);
                }
                return acquired;
            }
        };
        final TransactionalAccessDelegate delegate = new TransactionalAccessDelegate((CollectionRegionImpl)localCollectionRegion,
                                                                                     validator);

        Callable<Void> pferCallable = new Callable<Void>() {
            public Void call() throws Exception {
                delegate.putFromLoad("k1", "v1", 0, null);
                return null;
            }
        };

        Callable<Void> removeCallable = new Callable<Void>() {
            public Void call() throws Exception {
                removeLatch.await();
                delegate.remove("k1");
                pferLatch.countDown();
                return null;
            }
        };

        ExecutorService executorService = Executors.newCachedThreadPool();
        Future<Void> pferFuture = executorService.submit(pferCallable);
        Future<Void> removeFuture = executorService.submit(removeCallable);

        pferFuture.get();
        removeFuture.get();

        assertFalse(localCache.containsKey("k1"));
    }

    /**
     * Test method for
     * {@link CollectionRegionAccessStrategy#putFromLoad(java.lang.Object, java.lang.Object, long, java.lang.Object)} .
     */
    public void testPutFromLoad() throws Exception {
        putFromLoadTest(false);
    }

    /**
     * Test method for
     * {@link CollectionRegionAccessStrategy#putFromLoad(java.lang.Object, java.lang.Object, long, java.lang.Object, boolean)} .
     */
    public void testPutFromLoadMinimal() throws Exception {
        putFromLoadTest(true);
    }

    /**
     * Simulate 2 nodes, both start, tx do a get, experience a cache miss, then 'read from db.' First does a putFromLoad, then an
     * evict (to represent a change). Second tries to do a putFromLoad with stale data (i.e. it took longer to read from the db).
     * Both commit their tx. Then both start a new tx and get. First should see the updated data; second should either see the
     * updated data (isInvalidation()( == false) or null (isInvalidation() == true).
     *
     * @param useMinimalAPI
     * @throws Exception
     */
    private void putFromLoadTest( final boolean useMinimalAPI ) throws Exception {

        final String KEY = KEY_BASE + testCount++;

        final CountDownLatch writeLatch1 = new CountDownLatch(1);
        final CountDownLatch writeLatch2 = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(2);

        Thread node1 = new Thread() {

            @Override
            public void run() {

                try {
                    long txTimestamp = System.currentTimeMillis();
                    BatchModeTransactionManager.getInstance().begin();

                    assertEquals("node1 starts clean", null, localAccessStrategy.get(KEY, txTimestamp));

                    writeLatch1.await();

                    if (useMinimalAPI) {
                        localAccessStrategy.putFromLoad(KEY, VALUE2, txTimestamp, new Integer(2), true);
                    } else {
                        localAccessStrategy.putFromLoad(KEY, VALUE2, txTimestamp, new Integer(2));
                    }

                    BatchModeTransactionManager.getInstance().commit();
                } catch (Exception e) {
                    LOG.error("node1 caught exception", e);
                    node1Exception = e;
                    rollback();
                } catch (AssertionFailedError e) {
                    node1Failure = e;
                    rollback();
                } finally {
                    // Let node2 write
                    writeLatch2.countDown();
                    completionLatch.countDown();
                }
            }
        };

        Thread node2 = new Thread() {

            @Override
            public void run() {

                try {
                    long txTimestamp = System.currentTimeMillis();
                    BatchModeTransactionManager.getInstance().begin();

                    assertNull("node2 starts clean", remoteAccessStrategy.get(KEY, txTimestamp));

                    // Let node1 write
                    writeLatch1.countDown();
                    // Wait for node1 to finish
                    writeLatch2.await();

                    // Let the first PFER propagate
                    sleep(200);

                    if (useMinimalAPI) {
                        remoteAccessStrategy.putFromLoad(KEY, VALUE1, txTimestamp, new Integer(1), true);
                    } else {
                        remoteAccessStrategy.putFromLoad(KEY, VALUE1, txTimestamp, new Integer(1));
                    }

                    BatchModeTransactionManager.getInstance().commit();
                } catch (Exception e) {
                    LOG.error("node2 caught exception", e);
                    node2Exception = e;
                    rollback();
                } catch (AssertionFailedError e) {
                    node2Failure = e;
                    rollback();
                } finally {
                    completionLatch.countDown();
                }
            }
        };

        node1.setDaemon(true);
        node2.setDaemon(true);

        node1.start();
        node2.start();

        assertTrue("Threads completed", completionLatch.await(2, TimeUnit.SECONDS));

        if (node1Failure != null) throw node1Failure;
        if (node2Failure != null) throw node2Failure;

        assertEquals("node1 saw no exceptions", null, node1Exception);
        assertEquals("node2 saw no exceptions", null, node2Exception);

        // let the final PFER propagate
        sleep(100);

        long txTimestamp = System.currentTimeMillis();
        String msg1 = "Correct node1 value";
        String msg2 = "Correct node2 value";
        Object expected1 = null;
        Object expected2 = null;
        if (isUsingInvalidation()) {
            // PFER does not generate any invalidation, so each node should
            // succeed. We count on database locking and Hibernate removing
            // the collection on any update to prevent the situation we have
            // here where the caches have inconsistent data
            expected1 = VALUE2;
            expected2 = VALUE1;
        } else {
            // the initial VALUE2 should prevent the node2 put
            expected1 = VALUE2;
            expected2 = VALUE2;
        }

        assertEquals(msg1, expected1, localAccessStrategy.get(KEY, txTimestamp));
        assertEquals(msg2, expected2, remoteAccessStrategy.get(KEY, txTimestamp));
    }

    /**
     * Test method for {@link CollectionRegionAccessStrategy#remove(java.lang.Object)}.
     */
    public void testRemove() {
        evictOrRemoveTest(false);
    }

    /**
     * Test method for {@link CollectionRegionAccessStrategy#removeAll()}.
     */
    public void testRemoveAll() {
        evictOrRemoveAllTest(false);
    }

    /**
     * Test method for {@link CollectionRegionAccessStrategy#evict(java.lang.Object)}. FIXME add testing of the
     * "immediately without regard for transaction isolation" bit in the CollectionRegionAccessStrategy API.
     */
    public void testEvict() {
        evictOrRemoveTest(true);
    }

    /**
     * Test method for {@link CollectionRegionAccessStrategy#evictAll()}. FIXME add testing of the
     * "immediately without regard for transaction isolation" bit in the CollectionRegionAccessStrategy API.
     */
    public void testEvictAll() {
        evictOrRemoveAllTest(true);
    }

    private void evictOrRemoveTest( boolean evict ) {

        final String KEY = KEY_BASE + testCount++;

        assertNull("local is clean", localAccessStrategy.get(KEY, System.currentTimeMillis()));
        assertNull("remote is clean", remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

        localAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
        assertEquals(VALUE1, localAccessStrategy.get(KEY, System.currentTimeMillis()));
        remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
        assertEquals(VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

        // Wait for async propagation
        sleep(250);

        if (evict) localAccessStrategy.evict(KEY);
        else localAccessStrategy.remove(KEY);

        assertEquals(null, localAccessStrategy.get(KEY, System.currentTimeMillis()));

        assertEquals(null, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));
    }

<<<<<<< HEAD
    private void evictOrRemoveAllTest( boolean evict ) {
=======
      private final String configResource;
      private final String configName;
      private String preferIPv4Stack;
      private ServiceRegistry serviceRegistry;
>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder

        final String KEY = KEY_BASE + testCount++;

        assertEquals(0, getValidKeyCount(localCache.keySet()));

        assertEquals(0, getValidKeyCount(remoteCache.keySet()));

        assertNull("local is clean", localAccessStrategy.get(KEY, System.currentTimeMillis()));
        assertNull("remote is clean", remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

<<<<<<< HEAD
        localAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
        assertEquals(VALUE1, localAccessStrategy.get(KEY, System.currentTimeMillis()));
        remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
        assertEquals(VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

        // Wait for async propagation
        sleep(250);

        if (evict) localAccessStrategy.evictAll();
        else localAccessStrategy.removeAll();

        // This should re-establish the region root node
        assertNull(localAccessStrategy.get(KEY, System.currentTimeMillis()));

        assertEquals(0, getValidKeyCount(localCache.keySet()));

        // Re-establishing the region root on the local node doesn't
        // propagate it to other nodes. Do a get on the remote node to re-establish
        assertEquals(null, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

        assertEquals(0, getValidKeyCount(remoteCache.keySet()));

        // Test whether the get above messes up the optimistic version
        remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
        assertEquals(VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

        assertEquals(1, getValidKeyCount(remoteCache.keySet()));

        // Wait for async propagation of the putFromLoad
        sleep(250);

        assertEquals("local is correct",
                     (isUsingInvalidation() ? null : VALUE1),
                     localAccessStrategy.get(KEY, System.currentTimeMillis()));
        assertEquals("remote is correct", VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));
    }

    private void rollback() {
        try {
            BatchModeTransactionManager.getInstance().rollback();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

    }

    private static class AccessStrategyTestSetup extends TestSetup {

        private static final String PREFER_IPV4STACK = "java.net.preferIPv4Stack";

        private final String configResource;
        private final String configName;
        private String preferIPv4Stack;
        private ServiceRegistry serviceRegistry;

        public AccessStrategyTestSetup( Test test,
                                        String configName ) {
            this(test, configName, null);
        }

        public AccessStrategyTestSetup( Test test,
                                        String configName,
                                        String configResource ) {
            super(test);
            this.configName = configName;
            this.configResource = configResource;
        }

        @Override
        protected void setUp() throws Exception {
            super.setUp();

            // Try to ensure we use IPv4; otherwise cluster formation is very slow
            preferIPv4Stack = System.getProperty(PREFER_IPV4STACK);
            System.setProperty(PREFER_IPV4STACK, "true");

<<<<<<< HEAD
            serviceRegistry = ServiceRegistryBuilder.buildServiceRegistry(Environment.getProperties());
=======
      private final String configResource;
      private final String configName;
      private String preferIPv4Stack;

      private ServiceRegistry localServiceRegistry;
      private ServiceRegistry remoteServiceRegistry;
>>>>>>> HHH-5949 - Migrate, complete and integrate TransactionFactory as a service

            localCfg = createConfiguration(configName, configResource);
            localRegionFactory = CacheTestUtil.startRegionFactory(serviceRegistry.getService(JdbcServices.class), localCfg);

            remoteCfg = createConfiguration(configName, configResource);
            remoteRegionFactory = CacheTestUtil.startRegionFactory(serviceRegistry.getService(JdbcServices.class), remoteCfg);
        }

        @Override
        protected void tearDown() throws Exception {
            try {
                super.tearDown();
            } finally {
                if (preferIPv4Stack == null) System.clearProperty(PREFER_IPV4STACK);
                else System.setProperty(PREFER_IPV4STACK, preferIPv4Stack);
            }

            try {
                if (localRegionFactory != null) localRegionFactory.stop();

<<<<<<< HEAD
                if (remoteRegionFactory != null) remoteRegionFactory.stop();
            } finally {
                if (serviceRegistry != null) {
                    ServiceRegistryBuilder.destroy(serviceRegistry);
                }
=======
         serviceRegistry = ServiceRegistryBuilder.buildServiceRegistry( Environment.getProperties() );

         localCfg = createConfiguration(configName, configResource);
         localRegionFactory = CacheTestUtil.startRegionFactory( serviceRegistry, localCfg );
=======
		  localCfg = createConfiguration(configName, configResource);
		  localServiceRegistry = ServiceRegistryBuilder.buildServiceRegistry( localCfg.getProperties() );
		  localRegionFactory = CacheTestUtil.startRegionFactory( localServiceRegistry, localCfg );
>>>>>>> HHH-5949 - Migrate, complete and integrate TransactionFactory as a service

		  remoteCfg = createConfiguration(configName, configResource);
		  remoteServiceRegistry = ServiceRegistryBuilder.buildServiceRegistry( remoteCfg.getProperties() );
		  remoteRegionFactory = CacheTestUtil.startRegionFactory( remoteServiceRegistry, remoteCfg );
      }

      @Override
      protected void tearDown() throws Exception {
         try {
            super.tearDown();
         } finally {
            if (preferIPv4Stack == null)
               System.clearProperty(PREFER_IPV4STACK);
            else
               System.setProperty(PREFER_IPV4STACK, preferIPv4Stack);
         }

		  try {
            if (localRegionFactory != null)
               localRegionFactory.stop();

            if (remoteRegionFactory != null)
               remoteRegionFactory.stop();
		  }
		  finally {
<<<<<<< HEAD
            if ( serviceRegistry != null ) {
				ServiceRegistryBuilder.destroy( serviceRegistry );
>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder
=======
            if ( localServiceRegistry != null ) {
				ServiceRegistryBuilder.destroy( localServiceRegistry );
            }
            if ( remoteServiceRegistry != null ) {
				ServiceRegistryBuilder.destroy( remoteServiceRegistry );
>>>>>>> HHH-5949 - Migrate, complete and integrate TransactionFactory as a service
            }
        }

    }
=======
=======
>>>>>>> HHH-9840 Change all kinds of CacheKey contract to a raw Object
	public static final String REGION_NAME = "test/com.foo.test";
	public static final String KEY_BASE = "KEY";
	public static final String VALUE1 = "VALUE1";
	public static final String VALUE2 = "VALUE2";

	protected static int testCount;

	protected NodeEnvironment localEnvironment;
	protected CollectionRegionImpl localCollectionRegion;
	protected CollectionRegionAccessStrategy localAccessStrategy;

	protected NodeEnvironment remoteEnvironment;
	protected CollectionRegionImpl remoteCollectionRegion;
	protected CollectionRegionAccessStrategy remoteAccessStrategy;

	protected boolean invalidation;
	protected boolean synchronous;

	protected Exception node1Exception;
	protected Exception node2Exception;

	protected AssertionFailedError node1Failure;
	protected AssertionFailedError node2Failure;

	protected abstract AccessType getAccessType();

	@Before
	public void prepareResources() throws Exception {
		// to mimic exactly the old code results, both environments here are exactly the same...
		StandardServiceRegistryBuilder ssrb = createStandardServiceRegistryBuilder( getConfigurationName() );
		localEnvironment = new NodeEnvironment( ssrb );
		localEnvironment.prepare();

		localCollectionRegion = localEnvironment.getCollectionRegion( REGION_NAME, getCacheDataDescription() );
		localAccessStrategy = localCollectionRegion.buildAccessStrategy( getAccessType() );

		invalidation = Caches.isInvalidationCache(localCollectionRegion.getCache());
		synchronous = Caches.isSynchronousCache(localCollectionRegion.getCache());

		// Sleep a bit to avoid concurrent FLUSH problem
		avoidConcurrentFlush();

		remoteEnvironment = new NodeEnvironment( ssrb );
		remoteEnvironment.prepare();

		remoteCollectionRegion = remoteEnvironment.getCollectionRegion( REGION_NAME, getCacheDataDescription() );
		remoteAccessStrategy = remoteCollectionRegion.buildAccessStrategy( getAccessType() );
	}

	protected abstract String getConfigurationName();

	protected static StandardServiceRegistryBuilder createStandardServiceRegistryBuilder(String configName) {
		final StandardServiceRegistryBuilder ssrb = CacheTestUtil.buildBaselineStandardServiceRegistryBuilder(
				REGION_PREFIX,
				InfinispanRegionFactory.class,
				true,
				false
		);
		ssrb.applySetting( InfinispanRegionFactory.ENTITY_CACHE_RESOURCE_PROP, configName );
		return ssrb;
	}

	protected CacheDataDescription getCacheDataDescription() {
		return new CacheDataDescriptionImpl( true, true, ComparableComparator.INSTANCE, null);
	}

	@After
	public void releaseResources() throws Exception {
		if ( localEnvironment != null ) {
			localEnvironment.release();
		}
		if ( remoteEnvironment != null ) {
			remoteEnvironment.release();
		}
	}

	protected boolean isUsingInvalidation() {
		return invalidation;
	}

	protected boolean isSynchronous() {
		return synchronous;
	}

	@Test
	public abstract void testCacheConfiguration();

	@Test
	public void testGetRegion() {
		assertEquals( "Correct region", localCollectionRegion, localAccessStrategy.getRegion() );
	}

	@Test
	public void testPutFromLoadRemoveDoesNotProduceStaleData() throws Exception {
		final CountDownLatch pferLatch = new CountDownLatch( 1 );
		final CountDownLatch removeLatch = new CountDownLatch( 1 );
      final TransactionManager remoteTm = remoteCollectionRegion.getTransactionManager();
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(false)) {
         @Override
         public void call() {
            PutFromLoadValidator validator = new PutFromLoadValidator(remoteCollectionRegion.getCache(), cm,
                  remoteTm, 20000) {
               @Override
               public boolean acquirePutFromLoadLock(Object key) {
                  boolean acquired = super.acquirePutFromLoadLock( key );
                  try {
                     removeLatch.countDown();
                     pferLatch.await( 2, TimeUnit.SECONDS );
                  }
                  catch (InterruptedException e) {
                     log.debug( "Interrupted" );
                     Thread.currentThread().interrupt();
                  }
                  catch (Exception e) {
                     log.error( "Error", e );
                     throw new RuntimeException( "Error", e );
                  }
                  return acquired;
               }
            };

            final TransactionalAccessDelegate delegate =
                  new TransactionalAccessDelegate(localCollectionRegion, validator);
            final TransactionManager localTm = localCollectionRegion.getTransactionManager();

            Callable<Void> pferCallable = new Callable<Void>() {
               public Void call() throws Exception {
                  delegate.putFromLoad( "k1", "v1", 0, null );
                  return null;
               }
            };

            Callable<Void> removeCallable = new Callable<Void>() {
               public Void call() throws Exception {
                  removeLatch.await();
                  Caches.withinTx(localTm, new Callable<Void>() {
                     @Override
                     public Void call() throws Exception {
                        delegate.remove("k1");
                        return null;
                     }
                  });
                  pferLatch.countDown();
                  return null;
               }
            };

            ExecutorService executorService = Executors.newCachedThreadPool();
            Future<Void> pferFuture = executorService.submit( pferCallable );
            Future<Void> removeFuture = executorService.submit( removeCallable );

            try {
               pferFuture.get();
               removeFuture.get();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }

            assertFalse(localCollectionRegion.getCache().containsKey("k1"));
         }
      });
	}

	@Test
	public void testPutFromLoad() throws Exception {
		putFromLoadTest( false );
	}

	@Test
	public void testPutFromLoadMinimal() throws Exception {
		putFromLoadTest( true );
	}

	private void putFromLoadTest(final boolean useMinimalAPI) throws Exception {

		final Object KEY = TestingKeyFactory.generateCollectionCacheKey( KEY_BASE + testCount++ );

		final CountDownLatch writeLatch1 = new CountDownLatch( 1 );
		final CountDownLatch writeLatch2 = new CountDownLatch( 1 );
		final CountDownLatch completionLatch = new CountDownLatch( 2 );

		Thread node1 = new Thread() {

			public void run() {

				try {
					long txTimestamp = System.currentTimeMillis();
					BatchModeTransactionManager.getInstance().begin();

					assertEquals( "node1 starts clean", null, localAccessStrategy.get(null, KEY, txTimestamp ) );

					writeLatch1.await();

					if ( useMinimalAPI ) {
						localAccessStrategy.putFromLoad(null, KEY, VALUE2, txTimestamp, new Integer( 2 ), true );
					}
					else {
						localAccessStrategy.putFromLoad(null, KEY, VALUE2, txTimestamp, new Integer( 2 ) );
					}

					BatchModeTransactionManager.getInstance().commit();
				}
				catch (Exception e) {
					log.error( "node1 caught exception", e );
					node1Exception = e;
					rollback();
				}
				catch (AssertionFailedError e) {
					node1Failure = e;
					rollback();
				}
				finally {
					// Let node2 write
					writeLatch2.countDown();
					completionLatch.countDown();
				}
			}
		};

		Thread node2 = new Thread() {

			public void run() {

				try {
					long txTimestamp = System.currentTimeMillis();
					BatchModeTransactionManager.getInstance().begin();

					assertNull( "node2 starts clean", remoteAccessStrategy.get(null, KEY, txTimestamp ) );

					// Let node1 write
					writeLatch1.countDown();
					// Wait for node1 to finish
					writeLatch2.await();

					// Let the first PFER propagate
					sleep( 200 );

					if ( useMinimalAPI ) {
						remoteAccessStrategy.putFromLoad(null, KEY, VALUE1, txTimestamp, new Integer( 1 ), true );
					}
					else {
						remoteAccessStrategy.putFromLoad(null, KEY, VALUE1, txTimestamp, new Integer( 1 ) );
					}

					BatchModeTransactionManager.getInstance().commit();
				}
				catch (Exception e) {
					log.error( "node2 caught exception", e );
					node2Exception = e;
					rollback();
				}
				catch (AssertionFailedError e) {
					node2Failure = e;
					rollback();
				}
				finally {
					completionLatch.countDown();
				}
			}
		};

		node1.setDaemon( true );
		node2.setDaemon( true );

		node1.start();
		node2.start();

		assertTrue( "Threads completed", completionLatch.await( 2, TimeUnit.SECONDS ) );

		if ( node1Failure != null ) {
			throw node1Failure;
		}
		if ( node2Failure != null ) {
			throw node2Failure;
		}

		assertEquals( "node1 saw no exceptions", null, node1Exception );
		assertEquals( "node2 saw no exceptions", null, node2Exception );

		// let the final PFER propagate
		sleep( 100 );

		long txTimestamp = System.currentTimeMillis();
		String msg1 = "Correct node1 value";
		String msg2 = "Correct node2 value";
		Object expected1 = null;
		Object expected2 = null;
		if ( isUsingInvalidation() ) {
			// PFER does not generate any invalidation, so each node should
			// succeed. We count on database locking and Hibernate removing
			// the collection on any update to prevent the situation we have
			// here where the caches have inconsistent data
			expected1 = VALUE2;
			expected2 = VALUE1;
		}
		else {
			// the initial VALUE2 should prevent the node2 put
			expected1 = VALUE2;
			expected2 = VALUE2;
		}

		assertEquals( msg1, expected1, localAccessStrategy.get(null, KEY, txTimestamp ) );
		assertEquals( msg2, expected2, remoteAccessStrategy.get(null, KEY, txTimestamp ) );
	}

	@Test
	public void testRemove() throws Exception {
		evictOrRemoveTest( false );
	}

	@Test
	public void testRemoveAll() throws Exception {
		evictOrRemoveAllTest( false );
	}

	@Test
	public void testEvict() throws Exception {
		evictOrRemoveTest( true );
	}

	@Test
	public void testEvictAll() throws Exception {
		evictOrRemoveAllTest( true );
	}

	private void evictOrRemoveTest(final boolean evict) throws Exception {

		final Object KEY = TestingKeyFactory.generateCollectionCacheKey( KEY_BASE + testCount++ );

		assertNull( "local is clean", localAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );
		assertNull( "remote is clean", remoteAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );

		localAccessStrategy.putFromLoad(null, KEY, VALUE1, System.currentTimeMillis(), new Integer( 1 ) );
		assertEquals( VALUE1, localAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );
		remoteAccessStrategy.putFromLoad(null, KEY, VALUE1, System.currentTimeMillis(), new Integer( 1 ) );
		assertEquals( VALUE1, remoteAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );

		// Wait for async propagation
		sleep( 250 );

      Caches.withinTx(localCollectionRegion.getTransactionManager(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            if (evict)
               localAccessStrategy.evict(KEY);
            else
               localAccessStrategy.remove(null, KEY);
            return null;
         }
      });

		assertEquals( null, localAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );

		assertEquals( null, remoteAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );
	}

	private void evictOrRemoveAllTest(final boolean evict) throws Exception {

		final Object KEY = TestingKeyFactory.generateCollectionCacheKey( KEY_BASE + testCount++ );

		assertEquals( 0, getValidKeyCount( localCollectionRegion.getCache().keySet() ) );

		assertEquals( 0, getValidKeyCount( remoteCollectionRegion.getCache().keySet() ) );

		assertNull( "local is clean", localAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );
		assertNull( "remote is clean", remoteAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );

		localAccessStrategy.putFromLoad(null, KEY, VALUE1, System.currentTimeMillis(), new Integer( 1 ) );
		assertEquals( VALUE1, localAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );
		remoteAccessStrategy.putFromLoad(null, KEY, VALUE1, System.currentTimeMillis(), new Integer( 1 ) );
		assertEquals( VALUE1, remoteAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );

		// Wait for async propagation
		sleep( 250 );

      Caches.withinTx(localCollectionRegion.getTransactionManager(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            if (evict)
               localAccessStrategy.evictAll();
            else
               localAccessStrategy.removeAll();
            return null;
         }
      });

		// This should re-establish the region root node
		assertNull( localAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );

		assertEquals( 0, getValidKeyCount( localCollectionRegion.getCache().keySet() ) );

		// Re-establishing the region root on the local node doesn't
		// propagate it to other nodes. Do a get on the remote node to re-establish
		assertEquals( null, remoteAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );

		assertEquals( 0, getValidKeyCount( remoteCollectionRegion.getCache().keySet() ) );

		// Test whether the get above messes up the optimistic version
		remoteAccessStrategy.putFromLoad(null, KEY, VALUE1, System.currentTimeMillis(), new Integer( 1 ) );
		assertEquals( VALUE1, remoteAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );

		assertEquals( 1, getValidKeyCount( remoteCollectionRegion.getCache().keySet() ) );

		// Wait for async propagation of the putFromLoad
		sleep( 250 );

		assertEquals(
				"local is correct", (isUsingInvalidation() ? null : VALUE1), localAccessStrategy.get(
						null, KEY, System
				.currentTimeMillis()
		)
		);
		assertEquals( "remote is correct", VALUE1, remoteAccessStrategy.get(null, KEY, System.currentTimeMillis() ) );
	}

	private void rollback() {
		try {
			BatchModeTransactionManager.getInstance().rollback();
		}
		catch (Exception e) {
			log.error( e.getMessage(), e );
		}

	}
>>>>>>> HHH-5942 - Migrate to JUnit 4

}
