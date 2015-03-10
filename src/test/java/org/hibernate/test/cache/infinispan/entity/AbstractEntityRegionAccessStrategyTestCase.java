/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2007, Red Hat, Inc. and/or it's affiliates or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat, Inc. and/or it's affiliates.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.hibernate.test.cache.infinispan.entity;
<<<<<<< HEAD
<<<<<<< HEAD
=======

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder
import static org.hibernate.TestLogger.LOG;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
=======

>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder
import junit.extensions.TestSetup;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestSuite;
<<<<<<< HEAD
=======
import org.infinispan.transaction.tm.BatchModeTransactionManager;

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder
=======
=======
=======
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
>>>>>>> ISPN-6955 Add guarantees that the cluster forms
=======
import java.util.Arrays;
>>>>>>> HHH-7197 reimport imports
import java.util.concurrent.Callable;
>>>>>>> HHH-6955 Upgrade to Infinispan 5.1.0.CR3
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

<<<<<<< HEAD
import junit.framework.AssertionFailedError;
import org.hibernate.cache.infinispan.util.Caches;
import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;
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
import org.hibernate.cache.access.EntityRegionAccessStrategy;
import org.hibernate.cache.impl.CacheDataDescriptionImpl;
=======
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
import org.hibernate.cache.internal.CacheDataDescriptionImpl;
>>>>>>> HHH-6191 - repackage org.hibernate.cache per api/spi/internal split
import org.hibernate.cache.infinispan.InfinispanRegionFactory;
import org.hibernate.cache.infinispan.entity.EntityRegionImpl;
=======
=======
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
>>>>>>> HHH-9490 - Migrate from dom4j to jaxb for XML processing;
import org.hibernate.cache.infinispan.InfinispanRegionFactory;
import org.hibernate.cache.infinispan.entity.EntityRegionImpl;
import org.hibernate.cache.infinispan.util.Caches;
import org.hibernate.cache.internal.CacheDataDescriptionImpl;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
<<<<<<< HEAD
>>>>>>> HHH-7197 reimport imports
import org.hibernate.cfg.Configuration;
<<<<<<< HEAD
import org.hibernate.service.spi.ServiceRegistry;
=======
import org.hibernate.internal.util.compare.ComparableComparator;
<<<<<<< HEAD

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import junit.framework.AssertionFailedError;

>>>>>>> HHH-5942 - Migrate to JUnit 4
=======
>>>>>>> HHH-7197 reimport imports
import org.hibernate.test.cache.infinispan.AbstractNonFunctionalTestCase;
import org.hibernate.test.cache.infinispan.NodeEnvironment;
import org.hibernate.test.cache.infinispan.util.CacheTestUtil;
<<<<<<< HEAD
import org.hibernate.testing.ServiceRegistryBuilder;
import org.hibernate.internal.util.compare.ComparableComparator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
=======
=======
import org.hibernate.internal.util.compare.ComparableComparator;

import org.hibernate.test.cache.infinispan.AbstractNonFunctionalTestCase;
import org.hibernate.test.cache.infinispan.NodeEnvironment;
import org.hibernate.test.cache.infinispan.util.CacheTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import junit.framework.AssertionFailedError;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.tm.BatchModeTransactionManager;

import org.jboss.logging.Logger;
>>>>>>> HHH-9490 - Migrate from dom4j to jaxb for XML processing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
>>>>>>> HHH-5942 - Migrate to JUnit 4

/**
 * Base class for tests of EntityRegionAccessStrategy impls.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public abstract class AbstractEntityRegionAccessStrategyTestCase extends AbstractNonFunctionalTestCase {

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

    protected boolean invalidation;
    protected boolean synchronous;

    protected EntityRegion localEntityRegion;
    protected EntityRegionAccessStrategy localAccessStrategy;

    protected EntityRegion remoteEntityRegion;
    protected EntityRegionAccessStrategy remoteAccessStrategy;

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
    public AbstractEntityRegionAccessStrategyTestCase( String name ) {
        super(name);
    }

    protected abstract AccessType getAccessType();

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        // Sleep a bit to avoid concurrent FLUSH problem
        avoidConcurrentFlush();

        localEntityRegion = localRegionFactory.buildEntityRegion(REGION_NAME, localCfg.getProperties(), getCacheDataDescription());
        localAccessStrategy = localEntityRegion.buildAccessStrategy(getAccessType());

        localCache = ((BaseRegion)localEntityRegion).getCacheAdapter();

        invalidation = localCache.isClusteredInvalidation();
        synchronous = localCache.isSynchronous();

        // Sleep a bit to avoid concurrent FLUSH problem
        avoidConcurrentFlush();

        remoteEntityRegion = remoteRegionFactory.buildEntityRegion(REGION_NAME,
                                                                   remoteCfg.getProperties(),
                                                                   getCacheDataDescription());
        remoteAccessStrategy = remoteEntityRegion.buildAccessStrategy(getAccessType());

        remoteCache = ((BaseRegion)remoteEntityRegion).getCacheAdapter();

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

    protected static Configuration createConfiguration( String configName ) {
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

    protected void assertThreadsRanCleanly() {
        if (node1Failure != null) throw node1Failure;
        if (node2Failure != null) throw node2Failure;

        if (node1Exception != null) {
            LOG.error("node1 saw an exception", node1Exception);
            assertEquals("node1 saw no exceptions", null, node1Exception);
        }

        if (node2Exception != null) {
            LOG.error("node2 saw an exception", node2Exception);
            assertEquals("node2 saw no exceptions", null, node2Exception);
        }
    }

    /**
     * This is just a setup test where we assert that the cache config is as we expected.
     */
    public abstract void testCacheConfiguration();

    /**
     * Test method for {@link TransactionalAccess#getRegion()}.
     */
    public void testGetRegion() {
        assertEquals("Correct region", localEntityRegion, localAccessStrategy.getRegion());
    }

    /**
     * Test method for {@link TransactionalAccess#putFromLoad(java.lang.Object, java.lang.Object, long, java.lang.Object)} .
     */
    public void testPutFromLoad() throws Exception {
        putFromLoadTest(false);
    }

    /**
     * Test method for {@link TransactionalAccess#putFromLoad(java.lang.Object, java.lang.Object, long, java.lang.Object, boolean)}
     * .
     */
    public void testPutFromLoadMinimal() throws Exception {
        putFromLoadTest(true);
    }

    /**
     * Simulate 2 nodes, both start, tx do a get, experience a cache miss, then 'read from db.' First does a putFromLoad, then an
     * update. Second tries to do a putFromLoad with stale data (i.e. it took longer to read from the db). Both commit their tx.
     * Then both start a new tx and get. First should see the updated data; second should either see the updated data
     * (isInvalidation() == false) or null (isInvalidation() == true).
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

                    assertNull("node1 starts clean", localAccessStrategy.get(KEY, txTimestamp));

                    writeLatch1.await();

                    if (useMinimalAPI) {
                        localAccessStrategy.putFromLoad(KEY, VALUE1, txTimestamp, new Integer(1), true);
                    } else {
                        localAccessStrategy.putFromLoad(KEY, VALUE1, txTimestamp, new Integer(1));
                    }

                    localAccessStrategy.update(KEY, VALUE2, new Integer(2), new Integer(1));

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

                    assertNull("node1 starts clean", remoteAccessStrategy.get(KEY, txTimestamp));

                    // Let node1 write
                    writeLatch1.countDown();
                    // Wait for node1 to finish
                    writeLatch2.await();

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

        assertThreadsRanCleanly();

        long txTimestamp = System.currentTimeMillis();
        assertEquals("Correct node1 value", VALUE2, localAccessStrategy.get(KEY, txTimestamp));

        if (isUsingInvalidation()) {
            // no data version to prevent the PFER; we count on db locks preventing this
            assertEquals("Expected node2 value", VALUE1, remoteAccessStrategy.get(KEY, txTimestamp));
        } else {
            // The node1 update is replicated, preventing the node2 PFER
            assertEquals("Correct node2 value", VALUE2, remoteAccessStrategy.get(KEY, txTimestamp));
        }
    }

    /**
     * Test method for {@link TransactionalAccess#insert(java.lang.Object, java.lang.Object, java.lang.Object)}.
     */
    public void testInsert() throws Exception {

        final String KEY = KEY_BASE + testCount++;

        final CountDownLatch readLatch = new CountDownLatch(1);
        final CountDownLatch commitLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(2);

        Thread inserter = new Thread() {

            @Override
            public void run() {

                try {
                    long txTimestamp = System.currentTimeMillis();
                    BatchModeTransactionManager.getInstance().begin();

                    assertNull("Correct initial value", localAccessStrategy.get(KEY, txTimestamp));

                    localAccessStrategy.insert(KEY, VALUE1, new Integer(1));

                    readLatch.countDown();
                    commitLatch.await();

                    BatchModeTransactionManager.getInstance().commit();
                } catch (Exception e) {
                    LOG.error("node1 caught exception", e);
                    node1Exception = e;
                    rollback();
                } catch (AssertionFailedError e) {
                    node1Failure = e;
                    rollback();
                } finally {
                    completionLatch.countDown();
                }
            }
        };

        Thread reader = new Thread() {

            @Override
            public void run() {

                try {
                    long txTimestamp = System.currentTimeMillis();
                    BatchModeTransactionManager.getInstance().begin();

                    readLatch.await();
                    // Object expected = !isBlockingReads() ? null : VALUE1;
                    Object expected = null;

                    assertEquals("Correct initial value", expected, localAccessStrategy.get(KEY, txTimestamp));

                    BatchModeTransactionManager.getInstance().commit();
                } catch (Exception e) {
                    LOG.error("node1 caught exception", e);
                    node1Exception = e;
                    rollback();
                } catch (AssertionFailedError e) {
                    node1Failure = e;
                    rollback();
                } finally {
                    commitLatch.countDown();
                    completionLatch.countDown();
                }
            }
        };

        inserter.setDaemon(true);
        reader.setDaemon(true);
        inserter.start();
        reader.start();

        assertTrue("Threads completed", completionLatch.await(1, TimeUnit.SECONDS));

        assertThreadsRanCleanly();

        long txTimestamp = System.currentTimeMillis();
        assertEquals("Correct node1 value", VALUE1, localAccessStrategy.get(KEY, txTimestamp));
        Object expected = isUsingInvalidation() ? null : VALUE1;
        assertEquals("Correct node2 value", expected, remoteAccessStrategy.get(KEY, txTimestamp));
    }

    /**
     * Test method for {@link TransactionalAccess#update(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)} .
     */
    public void testUpdate() throws Exception {

        final String KEY = KEY_BASE + testCount++;

        // Set up initial state
        localAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
        remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));

        // Let the async put propagate
        sleep(250);

        final CountDownLatch readLatch = new CountDownLatch(1);
        final CountDownLatch commitLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(2);

        Thread updater = new Thread("testUpdate-updater") {

            @Override
            public void run() {
                boolean readerUnlocked = false;
                try {
                    long txTimestamp = System.currentTimeMillis();
                    BatchModeTransactionManager.getInstance().begin();
                    LOG.debug("Transaction began, get initial value");
                    assertEquals("Correct initial value", VALUE1, localAccessStrategy.get(KEY, txTimestamp));
                    LOG.debug("Now update value");
                    localAccessStrategy.update(KEY, VALUE2, new Integer(2), new Integer(1));
                    LOG.debug("Notify the read latch");
                    readLatch.countDown();
                    readerUnlocked = true;
                    LOG.debug("Await commit");
                    commitLatch.await();
                    BatchModeTransactionManager.getInstance().commit();
                } catch (Exception e) {
                    LOG.error("node1 caught exception", e);
                    node1Exception = e;
                    rollback();
                } catch (AssertionFailedError e) {
                    node1Failure = e;
                    rollback();
                } finally {
                    if (!readerUnlocked) readLatch.countDown();
                    LOG.debug("Completion latch countdown");
                    completionLatch.countDown();
                }
            }
        };

        Thread reader = new Thread("testUpdate-reader") {

            @Override
            public void run() {
                try {
                    long txTimestamp = System.currentTimeMillis();
                    BatchModeTransactionManager.getInstance().begin();
                    LOG.debug("Transaction began, await read latch");
                    readLatch.await();
                    LOG.debug("Read latch acquired, verify local access strategy");

                    // This won't block w/ mvc and will read the old value
                    Object expected = VALUE1;
                    assertEquals("Correct value", expected, localAccessStrategy.get(KEY, txTimestamp));

                    BatchModeTransactionManager.getInstance().commit();
                } catch (Exception e) {
                    LOG.error("node1 caught exception", e);
                    node1Exception = e;
                    rollback();
                } catch (AssertionFailedError e) {
                    node1Failure = e;
                    rollback();
                } finally {
                    commitLatch.countDown();
                    LOG.debug("Completion latch countdown");
                    completionLatch.countDown();
                }
            }
        };

        updater.setDaemon(true);
        reader.setDaemon(true);
        updater.start();
        reader.start();

        // Should complete promptly
        assertTrue(completionLatch.await(2, TimeUnit.SECONDS));

        assertThreadsRanCleanly();

        long txTimestamp = System.currentTimeMillis();
        assertEquals("Correct node1 value", VALUE2, localAccessStrategy.get(KEY, txTimestamp));
        Object expected = isUsingInvalidation() ? null : VALUE2;
        assertEquals("Correct node2 value", expected, remoteAccessStrategy.get(KEY, txTimestamp));
    }

    /**
     * Test method for {@link TransactionalAccess#remove(java.lang.Object)}.
     */
    public void testRemove() {
        evictOrRemoveTest(false);
    }

    /**
     * Test method for {@link TransactionalAccess#removeAll()}.
     */
    public void testRemoveAll() {
        evictOrRemoveAllTest(false);
    }

    /**
     * Test method for {@link TransactionalAccess#evict(java.lang.Object)}. FIXME add testing of the
     * "immediately without regard for transaction isolation" bit in the EntityRegionAccessStrategy API.
     */
    public void testEvict() {
        evictOrRemoveTest(true);
    }

    /**
     * Test method for {@link TransactionalAccess#evictAll()}. FIXME add testing of the
     * "immediately without regard for transaction isolation" bit in the EntityRegionAccessStrategy API.
     */
    public void testEvictAll() {
        evictOrRemoveAllTest(true);
    }

    private void evictOrRemoveTest( boolean evict ) {
        final String KEY = KEY_BASE + testCount++;
        assertEquals(0, getValidKeyCount(localCache.keySet()));
        assertEquals(0, getValidKeyCount(remoteCache.keySet()));

        assertNull("local is clean", localAccessStrategy.get(KEY, System.currentTimeMillis()));
        assertNull("remote is clean", remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

        localAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
        assertEquals(VALUE1, localAccessStrategy.get(KEY, System.currentTimeMillis()));
        remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
        assertEquals(VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

        if (evict) localAccessStrategy.evict(KEY);
        else localAccessStrategy.remove(KEY);

        assertEquals(null, localAccessStrategy.get(KEY, System.currentTimeMillis()));
        assertEquals(0, getValidKeyCount(localCache.keySet()));
        assertEquals(null, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));
        assertEquals(0, getValidKeyCount(remoteCache.keySet()));
    }

    private void evictOrRemoveAllTest( boolean evict ) {
        final String KEY = KEY_BASE + testCount++;
        assertEquals(0, getValidKeyCount(localCache.keySet()));
        assertEquals(0, getValidKeyCount(remoteCache.keySet()));
        assertNull("local is clean", localAccessStrategy.get(KEY, System.currentTimeMillis()));
        assertNull("remote is clean", remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

        localAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
        assertEquals(VALUE1, localAccessStrategy.get(KEY, System.currentTimeMillis()));

        // Wait for async propagation
        sleep(250);

        remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
        assertEquals(VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

        // Wait for async propagation
        sleep(250);

        if (evict) {
            LOG.debug("Call evict all locally");
            localAccessStrategy.evictAll();
        } else {
            localAccessStrategy.removeAll();
        }

        // This should re-establish the region root node in the optimistic case
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

        // Wait for async propagation
        sleep(250);

        assertEquals("local is correct",
                     (isUsingInvalidation() ? null : VALUE1),
                     localAccessStrategy.get(KEY, System.currentTimeMillis()));
        assertEquals("remote is correct", VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));
    }

    protected void rollback() {
        try {
            BatchModeTransactionManager.getInstance().rollback();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private static class AccessStrategyTestSetup extends TestSetup {

        private static final String PREFER_IPV4STACK = "java.net.preferIPv4Stack";
        private final String configName;
        private String preferIPv4Stack;
        private ServiceRegistry serviceRegistry;

        public AccessStrategyTestSetup( Test test,
                                        String configName ) {
            super(test);
            this.configName = configName;
        }

        @Override
        protected void setUp() throws Exception {
            try {
                super.tearDown();
            } finally {
                if (preferIPv4Stack == null) System.clearProperty(PREFER_IPV4STACK);
                else System.setProperty(PREFER_IPV4STACK, preferIPv4Stack);
            }

            // Try to ensure we use IPv4; otherwise cluster formation is very slow
            preferIPv4Stack = System.getProperty(PREFER_IPV4STACK);
            System.setProperty(PREFER_IPV4STACK, "true");

            serviceRegistry = ServiceRegistryBuilder.buildServiceRegistry(Environment.getProperties());

            localCfg = createConfiguration(configName);
            localRegionFactory = CacheTestUtil.startRegionFactory(serviceRegistry.getService(JdbcServices.class), localCfg);

            remoteCfg = createConfiguration(configName);
            remoteRegionFactory = CacheTestUtil.startRegionFactory(serviceRegistry.getService(JdbcServices.class), remoteCfg);
        }

        @Override
        protected void tearDown() throws Exception {
            super.tearDown();

            try {
                if (localRegionFactory != null) localRegionFactory.stop();

                if (remoteRegionFactory != null) remoteRegionFactory.stop();
            } finally {
<<<<<<< HEAD
<<<<<<< HEAD
                if (serviceRegistryHolder != null) {
                    serviceRegistryHolder.destroy();
=======
                if (serviceRegistry != null) {
                    ServiceRegistryBuilder.destroy(serviceRegistry);
>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder
                }
=======
               commitLatch.countDown();
               log.debug("Completion latch countdown");
               completionLatch.countDown();
            }
         }
      };

      updater.setDaemon(true);
      reader.setDaemon(true);
      updater.start();
      reader.start();

      // Should complete promptly
      assertTrue(completionLatch.await(2, TimeUnit.SECONDS));

      assertThreadsRanCleanly();

      long txTimestamp = System.currentTimeMillis();
      assertEquals("Correct node1 value", VALUE2, localAccessStrategy.get(KEY, txTimestamp));
      Object expected = isUsingInvalidation() ? null : VALUE2;
      assertEquals("Correct node2 value", expected, remoteAccessStrategy.get(KEY, txTimestamp));
   }

   /**
    * Test method for {@link TransactionalAccess#remove(java.lang.Object)}.
    */
   public void testRemove() {
      evictOrRemoveTest(false);
   }

   /**
    * Test method for {@link TransactionalAccess#removeAll()}.
    */
   public void testRemoveAll() {
      evictOrRemoveAllTest(false);
   }

   /**
    * Test method for {@link TransactionalAccess#evict(java.lang.Object)}.
    * 
    * FIXME add testing of the "immediately without regard for transaction isolation" bit in the
    * EntityRegionAccessStrategy API.
    */
   public void testEvict() {
      evictOrRemoveTest(true);
   }

   /**
    * Test method for {@link TransactionalAccess#evictAll()}.
    * 
    * FIXME add testing of the "immediately without regard for transaction isolation" bit in the
    * EntityRegionAccessStrategy API.
    */
   public void testEvictAll() {
      evictOrRemoveAllTest(true);
   }

   private void evictOrRemoveTest(boolean evict) {
      final String KEY = KEY_BASE + testCount++;
      assertEquals(0, getValidKeyCount(localCache.keySet()));
      assertEquals(0, getValidKeyCount(remoteCache.keySet()));

      assertNull("local is clean", localAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertNull("remote is clean", remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

      localAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
      assertEquals(VALUE1, localAccessStrategy.get(KEY, System.currentTimeMillis()));
      remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
      assertEquals(VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

      if (evict)
         localAccessStrategy.evict(KEY);
      else
         localAccessStrategy.remove(KEY);

      assertEquals(null, localAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertEquals(0, getValidKeyCount(localCache.keySet()));
      assertEquals(null, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertEquals(0, getValidKeyCount(remoteCache.keySet()));
   }

   private void evictOrRemoveAllTest(boolean evict) {
      final String KEY = KEY_BASE + testCount++;
      assertEquals(0, getValidKeyCount(localCache.keySet()));
      assertEquals(0, getValidKeyCount(remoteCache.keySet()));
      assertNull("local is clean", localAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertNull("remote is clean", remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

      localAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
      assertEquals(VALUE1, localAccessStrategy.get(KEY, System.currentTimeMillis()));

      // Wait for async propagation
      sleep(250);

      remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
      assertEquals(VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

      // Wait for async propagation
      sleep(250);

      if (evict) {
         log.debug("Call evict all locally");
         localAccessStrategy.evictAll();
      } else {
         localAccessStrategy.removeAll();
      }

      // This should re-establish the region root node in the optimistic case
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

      // Wait for async propagation
      sleep(250);

      assertEquals("local is correct", (isUsingInvalidation() ? null : VALUE1), localAccessStrategy
               .get(KEY, System.currentTimeMillis()));
      assertEquals("remote is correct", VALUE1, remoteAccessStrategy.get(KEY, System
               .currentTimeMillis()));
   }

   protected void rollback() {
      try {
         BatchModeTransactionManager.getInstance().rollback();
      } catch (Exception e) {
         log.error(e.getMessage(), e);
      }
   }

   private static class AccessStrategyTestSetup extends TestSetup {

      private static final String PREFER_IPV4STACK = "java.net.preferIPv4Stack";
      private final String configName;
      private String preferIPv4Stack;
      private ServiceRegistry serviceRegistry;

      public AccessStrategyTestSetup(Test test, String configName) {
         super(test);
         this.configName = configName;
      }

      @Override
      protected void setUp() throws Exception {
         try {
            super.tearDown();
         } finally {
            if (preferIPv4Stack == null)
               System.clearProperty(PREFER_IPV4STACK);
            else
               System.setProperty(PREFER_IPV4STACK, preferIPv4Stack);
         }

         // Try to ensure we use IPv4; otherwise cluster formation is very slow
         preferIPv4Stack = System.getProperty(PREFER_IPV4STACK);
         System.setProperty(PREFER_IPV4STACK, "true");

		 serviceRegistry = ServiceRegistryBuilder.buildServiceRegistry( Environment.getProperties() );

         localCfg = createConfiguration(configName);
         localRegionFactory = CacheTestUtil.startRegionFactory( serviceRegistry, localCfg );

         remoteCfg = createConfiguration(configName);
         remoteRegionFactory = CacheTestUtil.startRegionFactory( serviceRegistry, remoteCfg );
      }

      @Override
      protected void tearDown() throws Exception {
         super.tearDown();

		  try {
            if (localRegionFactory != null)
               localRegionFactory.stop();

            if (remoteRegionFactory != null)
               remoteRegionFactory.stop();
		  }
		  finally {
            if ( serviceRegistry != null ) {
				ServiceRegistryBuilder.destroy( serviceRegistry );
>>>>>>> HHH-5765 - Replaced ServiceRegistryHolder with ServiceRegistryBuilder
            }
        }

    }
=======
	public static final String REGION_NAME = "test/com.foo.test";
	public static final String KEY_BASE = "KEY";
	public static final String VALUE1 = "VALUE1";
	public static final String VALUE2 = "VALUE2";
=======
   private static final Logger log = Logger.getLogger(AbstractEntityRegionAccessStrategyTestCase.class);
>>>>>>> HHH-7763 No need to clear caches when these are going to be stopped

   public static final String REGION_NAME = "test/com.foo.test";
   public static final String KEY_BASE = "KEY";
   public static final String VALUE1 = "VALUE1";
   public static final String VALUE2 = "VALUE2";

   protected static int testCount;

   protected NodeEnvironment localEnvironment;
   protected EntityRegionImpl localEntityRegion;
   protected EntityRegionAccessStrategy localAccessStrategy;

   protected NodeEnvironment remoteEnvironment;
   protected EntityRegionImpl remoteEntityRegion;
   protected EntityRegionAccessStrategy remoteAccessStrategy;

   protected boolean invalidation;
   protected boolean synchronous;

   protected Exception node1Exception;
   protected Exception node2Exception;

   protected AssertionFailedError node1Failure;
   protected AssertionFailedError node2Failure;

<<<<<<< HEAD
<<<<<<< HEAD
	public static Test getTestSetup(Test test, String configName) {
		return new AccessStrategyTestSetup( test, configName );
	}

	/**
	 * Create a new TransactionalAccessTestCase.
	 *
	 * @param name
	 */
	public AbstractEntityRegionAccessStrategyTestCase(String name) {
		super( name );
	}

	protected abstract AccessType getAccessType();

	protected void setUp() throws Exception {
		super.setUp();

		// Sleep a bit to avoid concurrent FLUSH problem
		avoidConcurrentFlush();

		localEntityRegion = localRegionFactory.buildEntityRegion(
				REGION_NAME, localCfg
				.getProperties(), getCacheDataDescription()
		);
=======
		localEntityRegion = localEnvironment.getEntityRegion( REGION_NAME, getCacheDataDescription() );
>>>>>>> HHH-5942 - Migrate to JUnit 4
		localAccessStrategy = localEntityRegion.buildAccessStrategy( getAccessType() );
=======
   @Before
   public void prepareResources() throws Exception {
      // to mimic exactly the old code results, both environments here are exactly the same...
      StandardServiceRegistryBuilder ssrb = createStandardServiceRegistryBuilder( getConfigurationName() );
      localEnvironment = new NodeEnvironment( ssrb );
      localEnvironment.prepare();
>>>>>>> HHH-7763 No need to clear caches when these are going to be stopped

      localEntityRegion = localEnvironment.getEntityRegion(REGION_NAME, getCacheDataDescription());
      localAccessStrategy = localEntityRegion.buildAccessStrategy(getAccessType());

      invalidation = Caches.isInvalidationCache(localEntityRegion.getCache());
      synchronous = Caches.isSynchronousCache(localEntityRegion.getCache());

      // Sleep a bit to avoid concurrent FLUSH problem
      avoidConcurrentFlush();

      remoteEnvironment = new NodeEnvironment( ssrb );
      remoteEnvironment.prepare();

      remoteEntityRegion = remoteEnvironment.getEntityRegion(REGION_NAME, getCacheDataDescription());
      remoteAccessStrategy = remoteEntityRegion.buildAccessStrategy(getAccessType());

      waitForClusterToForm(localEntityRegion.getCache(),
            remoteEntityRegion.getCache());
   }

<<<<<<< HEAD
<<<<<<< HEAD
	protected void tearDown() throws Exception {

		super.tearDown();

		try {
			localCache.withFlags( FlagAdapter.CACHE_MODE_LOCAL ).clear();
		}
		catch (Exception e) {
			log.error( "Problem purging local cache", e );
		}

		try {
			remoteCache.withFlags( FlagAdapter.CACHE_MODE_LOCAL ).clear();
		}
		catch (Exception e) {
			log.error( "Problem purging remote cache", e );
		}

		node1Exception = null;
		node2Exception = null;

		node1Failure = null;
		node2Failure = null;
	}
=======
=======
   protected void waitForClusterToForm(Cache... caches) {
      TestingUtil.blockUntilViewsReceived(10000, Arrays.asList(caches));
   }

<<<<<<< HEAD
>>>>>>> ISPN-6955 Add guarantees that the cluster forms
	protected abstract String getConfigurationName();
>>>>>>> HHH-5942 - Migrate to JUnit 4

	protected static Configuration createConfiguration(String configName) {
		Configuration cfg = CacheTestUtil.buildConfiguration(
				REGION_PREFIX,
				InfinispanRegionFactory.class,
				true,
				false
		);
		cfg.setProperty( InfinispanRegionFactory.ENTITY_CACHE_RESOURCE_PROP, configName );
		return cfg;
	}

	protected CacheDataDescription getCacheDataDescription() {
		return new CacheDataDescriptionImpl( true, true, ComparableComparator.INSTANCE );
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

	protected abstract AccessType getAccessType();

	protected boolean isUsingInvalidation() {
		return invalidation;
	}

	protected boolean isSynchronous() {
		return synchronous;
	}

	protected void assertThreadsRanCleanly() {
		if ( node1Failure != null ) {
			throw node1Failure;
		}
		if ( node2Failure != null ) {
			throw node2Failure;
		}

		if ( node1Exception != null ) {
<<<<<<< HEAD
			log.error( "node1 saw an exception", node1Exception );
=======
            log.error("node1 saw an exception", node1Exception);
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
			assertEquals( "node1 saw no exceptions", null, node1Exception );
		}

		if ( node2Exception != null ) {
<<<<<<< HEAD
			log.error( "node2 saw an exception", node2Exception );
=======
            log.error("node2 saw an exception", node2Exception);
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
			assertEquals( "node2 saw no exceptions", null, node2Exception );
		}
	}

	@Test
	public abstract void testCacheConfiguration();

	@Test
	public void testGetRegion() {
		assertEquals( "Correct region", localEntityRegion, localAccessStrategy.getRegion() );
	}

	@Test
	public void testPutFromLoad() throws Exception {
		putFromLoadTest( false );
	}

	@Test
	public void testPutFromLoadMinimal() throws Exception {
		putFromLoadTest( true );
	}

	/**
	 * Simulate 2 nodes, both start, tx do a get, experience a cache miss, then 'read from db.' First
	 * does a putFromLoad, then an update. Second tries to do a putFromLoad with stale data (i.e. it
	 * took longer to read from the db). Both commit their tx. Then both start a new tx and get.
	 * First should see the updated data; second should either see the updated data (isInvalidation()
	 * == false) or null (isInvalidation() == true).
	 *
	 * @param useMinimalAPI
	 * @throws Exception
	 */
	private void putFromLoadTest(final boolean useMinimalAPI) throws Exception {

		final String KEY = KEY_BASE + testCount++;

		final CountDownLatch writeLatch1 = new CountDownLatch( 1 );
		final CountDownLatch writeLatch2 = new CountDownLatch( 1 );
		final CountDownLatch completionLatch = new CountDownLatch( 2 );

		Thread node1 = new Thread() {

			public void run() {

				try {
					long txTimestamp = System.currentTimeMillis();
					BatchModeTransactionManager.getInstance().begin();

					assertNull( "node1 starts clean", localAccessStrategy.get( KEY, txTimestamp ) );

					writeLatch1.await();

					if ( useMinimalAPI ) {
						localAccessStrategy.putFromLoad( KEY, VALUE1, txTimestamp, new Integer( 1 ), true );
					}
					else {
						localAccessStrategy.putFromLoad( KEY, VALUE1, txTimestamp, new Integer( 1 ) );
					}

					localAccessStrategy.update( KEY, VALUE2, new Integer( 2 ), new Integer( 1 ) );

					BatchModeTransactionManager.getInstance().commit();
				}
				catch (Exception e) {
<<<<<<< HEAD
					log.error( "node1 caught exception", e );
=======
                    log.error("node1 caught exception", e);
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
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

					assertNull( "node1 starts clean", remoteAccessStrategy.get( KEY, txTimestamp ) );

					// Let node1 write
					writeLatch1.countDown();
					// Wait for node1 to finish
					writeLatch2.await();

					if ( useMinimalAPI ) {
						remoteAccessStrategy.putFromLoad( KEY, VALUE1, txTimestamp, new Integer( 1 ), true );
					}
					else {
						remoteAccessStrategy.putFromLoad( KEY, VALUE1, txTimestamp, new Integer( 1 ) );
					}

					BatchModeTransactionManager.getInstance().commit();
				}
				catch (Exception e) {
<<<<<<< HEAD
					log.error( "node2 caught exception", e );
=======
                    log.error("node2 caught exception", e);
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
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

		assertThreadsRanCleanly();

		long txTimestamp = System.currentTimeMillis();
		assertEquals( "Correct node1 value", VALUE2, localAccessStrategy.get( KEY, txTimestamp ) );

		if ( isUsingInvalidation() ) {
			// no data version to prevent the PFER; we count on db locks preventing this
			assertEquals( "Expected node2 value", VALUE1, remoteAccessStrategy.get( KEY, txTimestamp ) );
		}
		else {
			// The node1 update is replicated, preventing the node2 PFER
			assertEquals( "Correct node2 value", VALUE2, remoteAccessStrategy.get( KEY, txTimestamp ) );
		}
	}

	@Test
	public void testInsert() throws Exception {

		final String KEY = KEY_BASE + testCount++;

		final CountDownLatch readLatch = new CountDownLatch( 1 );
		final CountDownLatch commitLatch = new CountDownLatch( 1 );
		final CountDownLatch completionLatch = new CountDownLatch( 2 );

		Thread inserter = new Thread() {

			public void run() {

				try {
					long txTimestamp = System.currentTimeMillis();
					BatchModeTransactionManager.getInstance().begin();

					assertNull( "Correct initial value", localAccessStrategy.get( KEY, txTimestamp ) );

					localAccessStrategy.insert( KEY, VALUE1, new Integer( 1 ) );

					readLatch.countDown();
					commitLatch.await();

					BatchModeTransactionManager.getInstance().commit();
				}
				catch (Exception e) {
<<<<<<< HEAD
					log.error( "node1 caught exception", e );
=======
                    log.error("node1 caught exception", e);
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
					node1Exception = e;
					rollback();
				}
				catch (AssertionFailedError e) {
					node1Failure = e;
					rollback();
				}
				finally {
					completionLatch.countDown();
				}
			}
		};

		Thread reader = new Thread() {

			public void run() {

				try {
					long txTimestamp = System.currentTimeMillis();
					BatchModeTransactionManager.getInstance().begin();

					readLatch.await();
//               Object expected = !isBlockingReads() ? null : VALUE1;
					Object expected = null;

					assertEquals(
							"Correct initial value", expected, localAccessStrategy.get(
							KEY,
							txTimestamp
					)
					);

					BatchModeTransactionManager.getInstance().commit();
				}
				catch (Exception e) {
<<<<<<< HEAD
					log.error( "node1 caught exception", e );
=======
                    log.error("node1 caught exception", e);
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
					node1Exception = e;
					rollback();
				}
				catch (AssertionFailedError e) {
					node1Failure = e;
					rollback();
				}
				finally {
					commitLatch.countDown();
					completionLatch.countDown();
				}
			}
		};

		inserter.setDaemon( true );
		reader.setDaemon( true );
		inserter.start();
		reader.start();

		assertTrue( "Threads completed", completionLatch.await( 1, TimeUnit.SECONDS ) );

		assertThreadsRanCleanly();

		long txTimestamp = System.currentTimeMillis();
		assertEquals( "Correct node1 value", VALUE1, localAccessStrategy.get( KEY, txTimestamp ) );
		Object expected = isUsingInvalidation() ? null : VALUE1;
		assertEquals( "Correct node2 value", expected, remoteAccessStrategy.get( KEY, txTimestamp ) );
	}

	@Test
	public void testUpdate() throws Exception {

		final String KEY = KEY_BASE + testCount++;

		// Set up initial state
		localAccessStrategy.putFromLoad( KEY, VALUE1, System.currentTimeMillis(), new Integer( 1 ) );
		remoteAccessStrategy.putFromLoad( KEY, VALUE1, System.currentTimeMillis(), new Integer( 1 ) );

		// Let the async put propagate
		sleep( 250 );

		final CountDownLatch readLatch = new CountDownLatch( 1 );
		final CountDownLatch commitLatch = new CountDownLatch( 1 );
		final CountDownLatch completionLatch = new CountDownLatch( 2 );

		Thread updater = new Thread( "testUpdate-updater" ) {

			public void run() {
				boolean readerUnlocked = false;
				try {
					long txTimestamp = System.currentTimeMillis();
					BatchModeTransactionManager.getInstance().begin();
<<<<<<< HEAD
					log.debug( "Transaction began, get initial value" );
					assertEquals( "Correct initial value", VALUE1, localAccessStrategy.get( KEY, txTimestamp ) );
					log.debug( "Now update value" );
					localAccessStrategy.update( KEY, VALUE2, new Integer( 2 ), new Integer( 1 ) );
					log.debug( "Notify the read latch" );
					readLatch.countDown();
					readerUnlocked = true;
					log.debug( "Await commit" );
=======
                    log.debug("Transaction began, get initial value");
					assertEquals( "Correct initial value", VALUE1, localAccessStrategy.get( KEY, txTimestamp ) );
                    log.debug("Now update value");
					localAccessStrategy.update( KEY, VALUE2, new Integer( 2 ), new Integer( 1 ) );
                    log.debug("Notify the read latch");
					readLatch.countDown();
					readerUnlocked = true;
                    log.debug("Await commit");
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
					commitLatch.await();
					BatchModeTransactionManager.getInstance().commit();
				}
				catch (Exception e) {
<<<<<<< HEAD
					log.error( "node1 caught exception", e );
=======
                    log.error("node1 caught exception", e);
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
					node1Exception = e;
					rollback();
				}
				catch (AssertionFailedError e) {
					node1Failure = e;
					rollback();
				}
				finally {
					if ( !readerUnlocked ) {
						readLatch.countDown();
					}
<<<<<<< HEAD
					log.debug( "Completion latch countdown" );
=======
                    log.debug("Completion latch countdown");
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
					completionLatch.countDown();
				}
			}
		};

		Thread reader = new Thread( "testUpdate-reader" ) {

			public void run() {
				try {
					long txTimestamp = System.currentTimeMillis();
					BatchModeTransactionManager.getInstance().begin();
<<<<<<< HEAD
					log.debug( "Transaction began, await read latch" );
					readLatch.await();
					log.debug( "Read latch acquired, verify local access strategy" );
=======
                    log.debug("Transaction began, await read latch");
					readLatch.await();
                    log.debug("Read latch acquired, verify local access strategy");
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes

					// This won't block w/ mvc and will read the old value
					Object expected = VALUE1;
					assertEquals( "Correct value", expected, localAccessStrategy.get( KEY, txTimestamp ) );

					BatchModeTransactionManager.getInstance().commit();
				}
				catch (Exception e) {
<<<<<<< HEAD
					log.error( "node1 caught exception", e );
=======
                    log.error("node1 caught exception", e);
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
					node1Exception = e;
					rollback();
				}
				catch (AssertionFailedError e) {
					node1Failure = e;
					rollback();
				}
				finally {
					commitLatch.countDown();
<<<<<<< HEAD
					log.debug( "Completion latch countdown" );
=======
                    log.debug("Completion latch countdown");
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
					completionLatch.countDown();
				}
			}
		};

		updater.setDaemon( true );
		reader.setDaemon( true );
		updater.start();
		reader.start();

		// Should complete promptly
		assertTrue( completionLatch.await( 2, TimeUnit.SECONDS ) );

		assertThreadsRanCleanly();

		long txTimestamp = System.currentTimeMillis();
		assertEquals( "Correct node1 value", VALUE2, localAccessStrategy.get( KEY, txTimestamp ) );
		Object expected = isUsingInvalidation() ? null : VALUE2;
		assertEquals( "Correct node2 value", expected, remoteAccessStrategy.get( KEY, txTimestamp ) );
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
		final String KEY = KEY_BASE + testCount++;
		assertEquals( 0, getValidKeyCount( localEntityRegion.getCache().keySet() ) );
		assertEquals( 0, getValidKeyCount( remoteEntityRegion.getCache().keySet() ) );

		assertNull( "local is clean", localAccessStrategy.get( KEY, System.currentTimeMillis() ) );
		assertNull( "remote is clean", remoteAccessStrategy.get( KEY, System.currentTimeMillis() ) );

		localAccessStrategy.putFromLoad( KEY, VALUE1, System.currentTimeMillis(), new Integer( 1 ) );
		assertEquals( VALUE1, localAccessStrategy.get( KEY, System.currentTimeMillis() ) );
		remoteAccessStrategy.putFromLoad( KEY, VALUE1, System.currentTimeMillis(), new Integer( 1 ) );
		assertEquals( VALUE1, remoteAccessStrategy.get( KEY, System.currentTimeMillis() ) );
=======
   protected abstract String getConfigurationName();

   protected static StandardServiceRegistryBuilder createStandardServiceRegistryBuilder(String configName) {
      StandardServiceRegistryBuilder ssrb = CacheTestUtil.buildBaselineStandardServiceRegistryBuilder(
            REGION_PREFIX,
            InfinispanRegionFactory.class,
            true,
            false
      );
      ssrb.applySetting( InfinispanRegionFactory.ENTITY_CACHE_RESOURCE_PROP, configName );
      return ssrb;
   }

   protected CacheDataDescription getCacheDataDescription() {
      return new CacheDataDescriptionImpl(true, true, ComparableComparator.INSTANCE);
   }

   @After
   public void releaseResources() throws Exception {
      try {
         if (localEnvironment != null) {
            localEnvironment.release();
         }
      } finally {
         if (remoteEnvironment != null) {
            remoteEnvironment.release();
         }
      }
   }

   protected abstract AccessType getAccessType();

   protected boolean isUsingInvalidation() {
      return invalidation;
   }

   protected boolean isSynchronous() {
      return synchronous;
   }

   protected void assertThreadsRanCleanly() {
      if (node1Failure != null) {
         throw node1Failure;
      }
      if (node2Failure != null) {
         throw node2Failure;
      }

      if (node1Exception != null) {
         log.error("node1 saw an exception", node1Exception);
         assertEquals("node1 saw no exceptions", null, node1Exception);
      }

      if (node2Exception != null) {
         log.error("node2 saw an exception", node2Exception);
         assertEquals("node2 saw no exceptions", null, node2Exception);
      }
   }

   @Test
   public abstract void testCacheConfiguration();

   @Test
   public void testGetRegion() {
      assertEquals("Correct region", localEntityRegion, localAccessStrategy.getRegion());
   }

   @Test
   public void testPutFromLoad() throws Exception {
      putFromLoadTest(false);
   }

   @Test
   public void testPutFromLoadMinimal() throws Exception {
      putFromLoadTest(true);
   }

   /**
    * Simulate 2 nodes, both start, tx do a get, experience a cache miss, then
    * 'read from db.' First does a putFromLoad, then an update. Second tries to
    * do a putFromLoad with stale data (i.e. it took longer to read from the db).
    * Both commit their tx. Then both start a new tx and get. First should see
    * the updated data; second should either see the updated data
    * (isInvalidation() == false) or null (isInvalidation() == true).
    *
    * @param useMinimalAPI
    * @throws Exception
    */
   private void putFromLoadTest(final boolean useMinimalAPI) throws Exception {

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

               assertNull("node1 starts clean", localAccessStrategy.get(KEY, txTimestamp));

               writeLatch1.await();

               if (useMinimalAPI) {
                  localAccessStrategy.putFromLoad(KEY, VALUE1, txTimestamp, new Integer(1), true);
               } else {
                  localAccessStrategy.putFromLoad(KEY, VALUE1, txTimestamp, new Integer(1));
               }

               localAccessStrategy.update(KEY, VALUE2, new Integer(2), new Integer(1));

               BatchModeTransactionManager.getInstance().commit();
            } catch (Exception e) {
               log.error("node1 caught exception", e);
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

               assertNull("node1 starts clean", remoteAccessStrategy.get(KEY, txTimestamp));

               // Let node1 write
               writeLatch1.countDown();
               // Wait for node1 to finish
               writeLatch2.await();

               if (useMinimalAPI) {
                  remoteAccessStrategy.putFromLoad(KEY, VALUE1, txTimestamp, new Integer(1), true);
               } else {
                  remoteAccessStrategy.putFromLoad(KEY, VALUE1, txTimestamp, new Integer(1));
               }

               BatchModeTransactionManager.getInstance().commit();
            } catch (Exception e) {
               log.error("node2 caught exception", e);
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

      assertThreadsRanCleanly();

      long txTimestamp = System.currentTimeMillis();
      assertEquals("Correct node1 value", VALUE2, localAccessStrategy.get(KEY, txTimestamp));

      if (isUsingInvalidation()) {
         // no data version to prevent the PFER; we count on db locks preventing this
         assertEquals("Expected node2 value", VALUE1, remoteAccessStrategy.get(KEY, txTimestamp));
      } else {
         // The node1 update is replicated, preventing the node2 PFER
         assertEquals("Correct node2 value", VALUE2, remoteAccessStrategy.get(KEY, txTimestamp));
      }
   }

   @Test
   public void testInsert() throws Exception {

      final String KEY = KEY_BASE + testCount++;

      final CountDownLatch readLatch = new CountDownLatch(1);
      final CountDownLatch commitLatch = new CountDownLatch(1);
      final CountDownLatch completionLatch = new CountDownLatch(2);

      Thread inserter = new Thread() {

         @Override
         public void run() {

            try {
               long txTimestamp = System.currentTimeMillis();
               BatchModeTransactionManager.getInstance().begin();

               assertNull("Correct initial value", localAccessStrategy.get(KEY, txTimestamp));

               localAccessStrategy.insert(KEY, VALUE1, new Integer(1));

               readLatch.countDown();
               commitLatch.await();

               BatchModeTransactionManager.getInstance().commit();
            } catch (Exception e) {
               log.error("node1 caught exception", e);
               node1Exception = e;
               rollback();
            } catch (AssertionFailedError e) {
               node1Failure = e;
               rollback();
            } finally {
               completionLatch.countDown();
            }
         }
      };

      Thread reader = new Thread() {

         @Override
         public void run() {

            try {
               long txTimestamp = System.currentTimeMillis();
               BatchModeTransactionManager.getInstance().begin();

               readLatch.await();
//               Object expected = !isBlockingReads() ? null : VALUE1;
               Object expected = null;

               assertEquals(
                     "Correct initial value", expected, localAccessStrategy.get(
                     KEY,
                     txTimestamp
               )
               );

               BatchModeTransactionManager.getInstance().commit();
            } catch (Exception e) {
               log.error("node1 caught exception", e);
               node1Exception = e;
               rollback();
            } catch (AssertionFailedError e) {
               node1Failure = e;
               rollback();
            } finally {
               commitLatch.countDown();
               completionLatch.countDown();
            }
         }
      };

      inserter.setDaemon(true);
      reader.setDaemon(true);
      inserter.start();
      reader.start();

      assertTrue("Threads completed", completionLatch.await(1, TimeUnit.SECONDS));

      assertThreadsRanCleanly();

      long txTimestamp = System.currentTimeMillis();
      assertEquals("Correct node1 value", VALUE1, localAccessStrategy.get(KEY, txTimestamp));
      Object expected = isUsingInvalidation() ? null : VALUE1;
      assertEquals("Correct node2 value", expected, remoteAccessStrategy.get(KEY, txTimestamp));
   }

   @Test
   public void testUpdate() throws Exception {

      final String KEY = KEY_BASE + testCount++;

      // Set up initial state
      localAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
      remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));

      // Let the async put propagate
      sleep(250);

      final CountDownLatch readLatch = new CountDownLatch(1);
      final CountDownLatch commitLatch = new CountDownLatch(1);
      final CountDownLatch completionLatch = new CountDownLatch(2);

      Thread updater = new Thread("testUpdate-updater") {

         @Override
         public void run() {
            boolean readerUnlocked = false;
            try {
               long txTimestamp = System.currentTimeMillis();
               BatchModeTransactionManager.getInstance().begin();
               log.debug("Transaction began, get initial value");
               assertEquals("Correct initial value", VALUE1, localAccessStrategy.get(KEY, txTimestamp));
               log.debug("Now update value");
               localAccessStrategy.update(KEY, VALUE2, new Integer(2), new Integer(1));
               log.debug("Notify the read latch");
               readLatch.countDown();
               readerUnlocked = true;
               log.debug("Await commit");
               commitLatch.await();
               BatchModeTransactionManager.getInstance().commit();
            } catch (Exception e) {
               log.error("node1 caught exception", e);
               node1Exception = e;
               rollback();
            } catch (AssertionFailedError e) {
               node1Failure = e;
               rollback();
            } finally {
               if (!readerUnlocked) {
                  readLatch.countDown();
               }
               log.debug("Completion latch countdown");
               completionLatch.countDown();
            }
         }
      };

      Thread reader = new Thread("testUpdate-reader") {

         @Override
         public void run() {
            try {
               long txTimestamp = System.currentTimeMillis();
               BatchModeTransactionManager.getInstance().begin();
               log.debug("Transaction began, await read latch");
               readLatch.await();
               log.debug("Read latch acquired, verify local access strategy");

               // This won't block w/ mvc and will read the old value
               Object expected = VALUE1;
               assertEquals("Correct value", expected, localAccessStrategy.get(KEY, txTimestamp));

               BatchModeTransactionManager.getInstance().commit();
            } catch (Exception e) {
               log.error("node1 caught exception", e);
               node1Exception = e;
               rollback();
            } catch (AssertionFailedError e) {
               node1Failure = e;
               rollback();
            } finally {
               commitLatch.countDown();
               log.debug("Completion latch countdown");
               completionLatch.countDown();
            }
         }
      };

      updater.setDaemon(true);
      reader.setDaemon(true);
      updater.start();
      reader.start();

      // Should complete promptly
      assertTrue(completionLatch.await(2, TimeUnit.SECONDS));

      assertThreadsRanCleanly();

      long txTimestamp = System.currentTimeMillis();
      assertEquals("Correct node1 value", VALUE2, localAccessStrategy.get(KEY, txTimestamp));
      Object expected = isUsingInvalidation() ? null : VALUE2;
      assertEquals("Correct node2 value", expected, remoteAccessStrategy.get(KEY, txTimestamp));
   }

   @Test
   public void testRemove() throws Exception {
      evictOrRemoveTest(false);
   }

   @Test
   public void testRemoveAll() throws Exception {
      evictOrRemoveAllTest(false);
   }

   @Test
   public void testEvict() throws Exception {
      evictOrRemoveTest(true);
   }

   @Test
   public void testEvictAll() throws Exception {
      evictOrRemoveAllTest(true);
   }

   private void evictOrRemoveTest(final boolean evict) throws Exception {
      final String KEY = KEY_BASE + testCount++;
      assertEquals(0, getValidKeyCount(localEntityRegion.getCache().keySet()));
      assertEquals(0, getValidKeyCount(remoteEntityRegion.getCache().keySet()));

      assertNull("local is clean", localAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertNull("remote is clean", remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

      localAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
      assertEquals(VALUE1, localAccessStrategy.get(KEY, System.currentTimeMillis()));
      remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
      assertEquals(VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));
>>>>>>> HHH-7763 No need to clear caches when these are going to be stopped

      Caches.withinTx(localEntityRegion.getTransactionManager(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            if (evict)
               localAccessStrategy.evict(KEY);
            else
               localAccessStrategy.remove(KEY);
            return null;
         }
      });
      assertEquals(null, localAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertEquals(0, getValidKeyCount(localEntityRegion.getCache().keySet()));
      assertEquals(null, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertEquals(0, getValidKeyCount(remoteEntityRegion.getCache().keySet()));
   }

   private void evictOrRemoveAllTest(final boolean evict) throws Exception {
      final String KEY = KEY_BASE + testCount++;
      assertEquals(0, getValidKeyCount(localEntityRegion.getCache().keySet()));
      assertEquals(0, getValidKeyCount(remoteEntityRegion.getCache().keySet()));
      assertNull("local is clean", localAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertNull("remote is clean", remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

      localAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
      assertEquals(VALUE1, localAccessStrategy.get(KEY, System.currentTimeMillis()));

      // Wait for async propagation
      sleep(250);

      remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
      assertEquals(VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));

      // Wait for async propagation
      sleep(250);

<<<<<<< HEAD
<<<<<<< HEAD
		if ( evict ) {
<<<<<<< HEAD
			log.debug( "Call evict all locally" );
=======
            log.debug("Call evict all locally");
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
			localAccessStrategy.evictAll();
		}
		else {
			localAccessStrategy.removeAll();
		}
=======
      CacheHelper.withinTx(localEntityRegion.getTransactionManager(), new Callable<Void>() {
=======
      Caches.withinTx(localEntityRegion.getTransactionManager(), new Callable<Void>() {
>>>>>>> HHH-7640 Improve single node Infinispan 2LC performance
         @Override
         public Void call() throws Exception {
            if (evict) {
               log.debug("Call evict all locally");
               localAccessStrategy.evictAll();
            } else {
               localAccessStrategy.removeAll();
            }
            return null;
         }
      });
>>>>>>> HHH-6955 Upgrade to Infinispan 5.1.0.CR3

<<<<<<< HEAD
		// This should re-establish the region root node in the optimistic case
		assertNull(localAccessStrategy.get(KEY, System.currentTimeMillis()));
		assertEquals( 0, getValidKeyCount( localEntityRegion.getCache().keySet() ) );

		// Re-establishing the region root on the local node doesn't
		// propagate it to other nodes. Do a get on the remote node to re-establish
		assertEquals( null, remoteAccessStrategy.get( KEY, System.currentTimeMillis() ) );
		assertEquals( 0, getValidKeyCount( remoteEntityRegion.getCache().keySet() ) );

		// Test whether the get above messes up the optimistic version
		remoteAccessStrategy.putFromLoad( KEY, VALUE1, System.currentTimeMillis(), new Integer( 1 ) );
		assertEquals( VALUE1, remoteAccessStrategy.get( KEY, System.currentTimeMillis() ) );
		assertEquals( 1, getValidKeyCount( remoteEntityRegion.getCache().keySet() ) );

		// Wait for async propagation
		sleep( 250 );

		assertEquals(
				"local is correct", (isUsingInvalidation() ? null : VALUE1), localAccessStrategy
				.get( KEY, System.currentTimeMillis() )
		);
		assertEquals(
				"remote is correct", VALUE1, remoteAccessStrategy.get(
				KEY, System
				.currentTimeMillis()
		)
		);
	}

	protected void rollback() {
		try {
			BatchModeTransactionManager.getInstance().rollback();
		}
		catch (Exception e) {
<<<<<<< HEAD
			log.error( e.getMessage(), e );
=======
            log.error(e.getMessage(), e);
>>>>>>> HHH-6098 - Slight naming changes in regards to new logging classes
		}
	}
<<<<<<< HEAD

	private static class AccessStrategyTestSetup extends TestSetup {

		private static final String PREFER_IPV4STACK = "java.net.preferIPv4Stack";
		private final String configName;
		private String preferIPv4Stack;

		private ServiceRegistry localServiceRegistry;
		private ServiceRegistry remoteServiceRegistry;

		public AccessStrategyTestSetup(Test test, String configName) {
			super( test );
			this.configName = configName;
		}

		@Override
		protected void setUp() throws Exception {
			try {
				super.tearDown();
			}
			finally {
				if ( preferIPv4Stack == null ) {
					System.clearProperty( PREFER_IPV4STACK );
				}
				else {
					System.setProperty( PREFER_IPV4STACK, preferIPv4Stack );
				}
			}

			// Try to ensure we use IPv4; otherwise cluster formation is very slow
			preferIPv4Stack = System.getProperty( PREFER_IPV4STACK );
			System.setProperty( PREFER_IPV4STACK, "true" );


			localCfg = createConfiguration( configName );
			localServiceRegistry = ServiceRegistryBuilder.buildServiceRegistry( localCfg.getProperties() );
			localRegionFactory = CacheTestUtil.startRegionFactory( localServiceRegistry, localCfg );

			remoteCfg = createConfiguration( configName );
			remoteServiceRegistry = ServiceRegistryBuilder.buildServiceRegistry( remoteCfg.getProperties() );
			remoteRegionFactory = CacheTestUtil.startRegionFactory( remoteServiceRegistry, remoteCfg );
		}

		@Override
		protected void tearDown() throws Exception {
			super.tearDown();

			try {
				if ( localRegionFactory != null ) {
					localRegionFactory.stop();
				}

				if ( remoteRegionFactory != null ) {
					remoteRegionFactory.stop();
				}
			}
			finally {
				if ( localServiceRegistry != null ) {
					ServiceRegistryBuilder.destroy( localServiceRegistry );
				}
				if ( remoteServiceRegistry != null ) {
					ServiceRegistryBuilder.destroy( remoteServiceRegistry );
				}
			}
		}

	}
>>>>>>> HHH-5949 - Migrate, complete and integrate TransactionFactory as a service

=======
>>>>>>> HHH-5942 - Migrate to JUnit 4
=======
      // This should re-establish the region root node in the optimistic case
      assertNull(localAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertEquals(0, getValidKeyCount(localEntityRegion.getCache().keySet()));

      // Re-establishing the region root on the local node doesn't
      // propagate it to other nodes. Do a get on the remote node to re-establish
      assertEquals(null, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertEquals(0, getValidKeyCount(remoteEntityRegion.getCache().keySet()));

      // Test whether the get above messes up the optimistic version
      remoteAccessStrategy.putFromLoad(KEY, VALUE1, System.currentTimeMillis(), new Integer(1));
      assertEquals(VALUE1, remoteAccessStrategy.get(KEY, System.currentTimeMillis()));
      assertEquals(1, getValidKeyCount(remoteEntityRegion.getCache().keySet()));

      // Wait for async propagation
      sleep(250);

      assertEquals(
            "local is correct", (isUsingInvalidation() ? null : VALUE1), localAccessStrategy
            .get(KEY, System.currentTimeMillis())
      );
      assertEquals(
            "remote is correct", VALUE1, remoteAccessStrategy.get(
            KEY, System
            .currentTimeMillis()
      )
      );
   }

   protected void rollback() {
      try {
         BatchModeTransactionManager.getInstance().rollback();
      } catch (Exception e) {
         log.error(e.getMessage(), e);
      }
   }
>>>>>>> HHH-7763 No need to clear caches when these are going to be stopped
}
