package org.infinispan.xsite.irac;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commands.RequestUUID;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.xsite.XSiteAdminOperations;
import org.jgroups.UnreachableException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests the IRAC queue cleanup through {@link XSiteAdminOperations} with the real {@link DefaultIracManager}.
 * <p>
 * The backup site is unreachable and the back-off is never released during the test, so the keys stay queued.
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "xsite.irac.IracQueueCleanupTest")
public class IracQueueCleanupTest extends SingleCacheManagerTest {

   private static final String LON = "LON";
   private static final String NYC = "NYC";
   private static final String CACHE_NAME = "irac-queue-cleanup";

   private final ControlledExponentialBackOff backOff = new ControlledExponentialBackOff();
   private final AtomicLong requestId = new AtomicLong();
   private volatile ControlledTransport transport;
   private volatile DefaultIracManager iracManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager();
      transport = TestingUtil.wrapGlobalComponent(cacheManager, Transport.class,
            actual -> new ControlledTransport(actual, LON, Collections.singleton(NYC)), true);
      cache = cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(CACHE_NAME, cacheConfiguration().build());
      iracManager = (DefaultIracManager) TestingUtil.extractComponent(cache, IracManager.class);
      iracManager.setBackOff(backup -> backOff);
      iracManager.setQueueCleanupSettings(new IracQueueCleanup.Settings(0, 10, 2, 1));
      return cacheManager;
   }

   @AfterMethod(alwaysRun = true)
   public void dropQueue() {
      xSiteAdmin().cancelQueueCleanup();
      // a key without an entry and without a tombstone can be left in the queue; it never drains by itself.
      iracManager.trackClear(false);
      transport.throwableSupplier = () -> null;
      backOff.release();
      eventually(iracManager::isEmpty);
      backOff.cleanupEvents();
   }

   @Override
   protected void teardown() {
      super.teardown();
      iracManager = null;
      transport = null;
   }

   private static ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      builder.sites().addBackup()
            .site(NYC)
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      return builder;
   }

   public void testRemovesOnlyKeysWithoutEntryAndTombstone(Method method) {
      String inMemory = TestingUtil.k(method, "in-memory");
      String inStore = TestingUtil.k(method, "in-store");
      String orphan = TestingUtil.k(method, "orphan");
      holdQueue(inMemory);

      cache.put(inStore, "value");
      // the entry is only in the store; the cleanup must load it.
      TestingUtil.extractComponent(cache, InternalDataContainer.class).remove(segmentOf(inStore), inStore);
      trackWithoutEntry(orphan);

      Map<String, IracQueueCleanupStatus> started = xSiteAdmin().startQueueCleanup(0);
      assertEquals(1, started.size());
      eventually(() -> "COMPLETED".equals(localStatus().state()));

      assertFalse(iracManager.containsKey(orphan), "The key without an entry and without a tombstone must be removed");
      assertTrue(iracManager.containsKey(inMemory), "A key with an entry in memory must be kept");
      assertTrue(iracManager.containsKey(inStore), "A key with an entry in the store must be kept");
      IracQueueCleanupStatus status = localStatus();
      assertEquals(3, status.checked());
      assertEquals(1, status.candidates());
      assertEquals(1, status.removed());
      assertEquals(2, status.queueSize());
   }

   public void testKeepsKeysYoungerThanMinAge(Method method) {
      String orphan = TestingUtil.k(method, "orphan");
      holdQueue(TestingUtil.k(method, "in-memory"));
      trackWithoutEntry(orphan);

      xSiteAdmin().startQueueCleanup(Long.MAX_VALUE);
      eventually(() -> "COMPLETED".equals(localStatus().state()));

      assertTrue(iracManager.containsKey(orphan));
      assertEquals(0, localStatus().checked());
   }

   public void testCancel(Method method) {
      iracManager.setQueueCleanupSettings(new IracQueueCleanup.Settings(0, Long.MAX_VALUE / 2, 2, 1));
      try {
         String orphan = TestingUtil.k(method, "orphan");
         holdQueue(TestingUtil.k(method, "in-memory"));
         trackWithoutEntry(orphan);

         xSiteAdmin().startQueueCleanup(0);
         eventuallyEquals(1L, () -> localStatus().candidates());
         assertEquals("CANCELLED", xSiteAdmin().cancelQueueCleanup().values().iterator().next().state());

         assertTrue(iracManager.containsKey(orphan));
      } finally {
         iracManager.setQueueCleanupSettings(new IracQueueCleanup.Settings(0, 10, 2, 1));
      }
   }

   /**
    * Puts a key while the backup site is unreachable, so the backup enters the back-off. The back-off is not released
    * until the end of the test, so all keys stay queued.
    */
   private void holdQueue(String key) {
      transport.throwableSupplier = () -> new UnreachableException(null);
      cache.put(key, "value");
      backOff.eventually("The backup did not back-off.", ControlledExponentialBackOff.Event.BACK_OFF);
   }

   private void trackWithoutEntry(String key) {
      iracManager.trackUpdatedKey(segmentOf(key), key, RequestUUID.of(address(), requestId.incrementAndGet()));
      assertTrue(iracManager.containsKey(key));
   }

   private int segmentOf(Object key) {
      return TestingUtil.extractCacheTopology(cache).getSegment(key);
   }

   private Address address() {
      return cache.getCacheManager().getAddress();
   }

   private XSiteAdminOperations xSiteAdmin() {
      return TestingUtil.extractComponent(cache, XSiteAdminOperations.class);
   }

   private IracQueueCleanupStatus localStatus() {
      return xSiteAdmin().queueCleanupStatus().values().iterator().next();
   }
}
