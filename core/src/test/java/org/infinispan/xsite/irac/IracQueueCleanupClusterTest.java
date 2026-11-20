package org.infinispan.xsite.irac;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commands.RequestUUID;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.XSiteAdminOperations;
import org.jgroups.UnreachableException;
import org.testng.annotations.Test;

/**
 * Tests the IRAC queue cleanup in a cluster: the command reaches all members, only the primary owner checks the key,
 * and the backup owner's copy is removed by the existing cleanup command.
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "xsite.irac.IracQueueCleanupClusterTest")
public class IracQueueCleanupClusterTest extends MultipleCacheManagersTest {

   private static final String LON = "LON";
   private static final String NYC = "NYC";
   private static final String CACHE_NAME = "irac-queue-cleanup-cluster";

   private final ControlledExponentialBackOff backOff = new ControlledExponentialBackOff();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2);
      builder.sites().addBackup().site(NYC).strategy(BackupConfiguration.BackupStrategy.ASYNC);
      for (int i = 0; i < 2; ++i) {
         EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager();
         ControlledTransport transport = TestingUtil.wrapGlobalComponent(cacheManager, Transport.class,
               actual -> new ControlledTransport(actual, LON, Collections.singleton(NYC)), true);
         // the backup site is unreachable and the back-off is never released, so the keys stay queued.
         transport.throwableSupplier = () -> new UnreachableException(null);
         cacheManager.defineConfiguration(CACHE_NAME, builder.build());
      }
      waitForClusterToForm(CACHE_NAME);
      for (Cache<Object, Object> cache : this.<Object, Object>caches(CACHE_NAME)) {
         DefaultIracManager iracManager = iracManager(cache);
         iracManager.setBackOff(backup -> backOff);
         iracManager.setQueueCleanupSettings(new IracQueueCleanup.Settings(0, 10, 2, 1));
      }
   }

   public void testOrphanRemovedFromPrimaryAndBackupOwner() {
      Cache<Object, Object> primary = cache(0, CACHE_NAME);
      Cache<Object, Object> backupOwner = cache(1, CACHE_NAME);
      String orphan = getStringKeyForCache("orphan", primary);

      primary.put(getStringKeyForCache("in-memory", primary), "value");
      backOff.eventually("The primary did not back-off.", ControlledExponentialBackOff.Event.BACK_OFF);
      // both owners track a write; the entry is never stored.
      RequestUUID owner = RequestUUID.of(address(primary), 1);
      int segment = TestingUtil.extractCacheTopology(primary).getSegment(orphan);
      iracManager(primary).trackUpdatedKey(segment, orphan, owner);
      iracManager(backupOwner).trackUpdatedKey(segment, orphan, owner);

      // started from the node that is not the primary owner: the command must reach the primary owner.
      Map<String, IracQueueCleanupStatus> started = extractComponent(backupOwner, XSiteAdminOperations.class).startQueueCleanup(0);
      assertEquals(2, started.size());

      eventually(() -> !iracManager(primary).containsKey(orphan) && !iracManager(backupOwner).containsKey(orphan));
      Map<String, IracQueueCleanupStatus> status = extractComponent(backupOwner, XSiteAdminOperations.class).queueCleanupStatus();
      IracQueueCleanupStatus primaryStatus = status.get(String.valueOf(address(primary)));
      IracQueueCleanupStatus backupStatus = status.get(String.valueOf(address(backupOwner)));
      assertEquals(1, primaryStatus.removed());
      assertEquals(0, backupStatus.checked(), "A backup owner must not check the keys");
   }

   @Override
   protected void clearContent() {
      // the backup site is unreachable: drop the queues locally instead of clearing the caches.
      for (Cache<Object, Object> cache : this.<Object, Object>caches(CACHE_NAME)) {
         iracManager(cache).trackClear(false);
      }
   }

   private static DefaultIracManager iracManager(Cache<?, ?> cache) {
      return (DefaultIracManager) extractComponent(cache, IracManager.class);
   }
}
