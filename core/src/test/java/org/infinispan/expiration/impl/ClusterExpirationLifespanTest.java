package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.SkipException;
import org.testng.annotations.Test;

/**
 * Tests to make sure that when lifespan expiration occurs it occurs across the cluster
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "expiration.impl.ClusterExpirationLifespanTest")
public class ClusterExpirationLifespanTest extends MultipleCacheManagersTest {

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   protected ControlledTimeService ts0;
   protected ControlledTimeService ts1;
   protected ControlledTimeService ts2;

   protected Cache<Object, Object> cache0;
   protected Cache<Object, Object> cache1;
   protected Cache<Object, Object> cache2;

   protected ConfigurationBuilder configurationBuilder;

   @Override
   public Object[] factory() {
      return Arrays.stream(StorageType.values())
            .flatMap(type ->
               Stream.builder()
                     .add(new ClusterExpirationLifespanTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC))
                     .add(new ClusterExpirationLifespanTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC))
                     .add(new ClusterExpirationLifespanTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(false))
                     .add(new ClusterExpirationLifespanTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC))
                     .add(new ClusterExpirationLifespanTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC))
                     .add(new ClusterExpirationLifespanTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(false))
                     .build()
            ).toArray();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.clustering().cacheMode(cacheMode);
      configurationBuilder.transaction().transactionMode(transactionMode()).lockingMode(lockingMode);
      configurationBuilder.expiration().disableReaper();
      if (storageType != null) {
         configurationBuilder.memory().storage(storageType);
      }
      createCluster(TestDataSCI.INSTANCE, configurationBuilder, 3);
      waitForClusterToForm();
      injectTimeServices();

      cache0 = cache(0);
      cache1 = cache(1);
      cache2 = cache(2);
   }

   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationBuilder() {
      GlobalConfigurationBuilder globalConfigurationBuilder = super.defaultGlobalConfigurationBuilder();
      globalConfigurationBuilder
            .serialization().marshaller(new JavaSerializationMarshaller())
            .allowList().addClasses(ExpirationFunctionalTest.NoEquals.class, MagicKey.class);
      return globalConfigurationBuilder;
   }

   protected void injectTimeServices() {
      ts0 = new ControlledTimeService();
      TestingUtil.replaceComponent(manager(0), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService();
      TestingUtil.replaceComponent(manager(1), TimeService.class, ts1, true);
      ts2 = new ControlledTimeService();
      TestingUtil.replaceComponent(manager(2), TimeService.class, ts2, true);
   }

   public void testLifespanExpiredOnPrimaryOwner() throws Exception {
      testLifespanExpiredEntryRetrieval(cache0, cache1, ts0, true);
   }

   public void testLifespanExpiredOnBackupOwner() throws Exception {
      testLifespanExpiredEntryRetrieval(cache0, cache1, ts1, false);
   }

   private void testLifespanExpiredEntryRetrieval(Cache<Object, Object> primaryOwner, Cache<Object, Object> backupOwner,
         ControlledTimeService timeService, boolean expireOnPrimary) throws Exception {
      Object key = createKey(primaryOwner, backupOwner);
      primaryOwner.put(key, key.toString(), 10, TimeUnit.MILLISECONDS);

      assertEquals(key.toString(), primaryOwner.get(key));
      assertEquals(key.toString(), backupOwner.get(key));

      // Now we expire on cache0, it should still exist on cache1
      // Note this has to be within the buffer in RemoveExpiredCommand (100 ms the time of this commit)
      timeService.advance(11);

      Cache<?, ?> expiredCache;
      Cache<?, ?> otherCache;
      if (expireOnPrimary) {
         expiredCache = primaryOwner;
         otherCache = backupOwner;
      } else {
         expiredCache = backupOwner;
         otherCache = primaryOwner;
      }

      assertEquals(key.toString(), otherCache.get(key));

      // By calling get on an expired key it will remove it all over
      Object expiredValue = expiredCache.get(key);
      assertNull(expiredValue);
      // This should be expired on the other node soon - note expiration is done asynchronously on a get
      eventually(() -> !otherCache.containsKey(key), 10, TimeUnit.SECONDS);
   }

   private Object createKey(Cache<Object, ?> primaryOwner, Cache<Object, ?> backupOwner) {
      if (storageType == StorageType.OBJECT) {
         return new MagicKey(primaryOwner, backupOwner);
      } else {
         // BINARY and OFF heap can't use MagicKey as they are serialized
         LocalizedCacheTopology primaryLct = primaryOwner.getAdvancedCache().getDistributionManager().getCacheTopology();
         LocalizedCacheTopology backupLct = backupOwner.getAdvancedCache().getDistributionManager().getCacheTopology();
         ThreadLocalRandom tlr = ThreadLocalRandom.current();

         int attempt = 0;
         while (true) {
            int key = tlr.nextInt();
            // We test ownership based on the stored key instance
            Object wrappedKey = primaryOwner.getAdvancedCache().getKeyDataConversion().toStorage(key);
            if (primaryLct.getDistribution(wrappedKey).isPrimary() &&
                  backupLct.getDistribution(wrappedKey).isWriteBackup()) {
               log.tracef("Found key %s for primary owner %s and backup owner %s", wrappedKey, primaryOwner, backupOwner);
               // Return the actual key not the stored one, else it will be wrapped again :(
               return key;
            }
            if (++attempt == 1_000) {
               throw new AssertionError("Unable to find key that maps to primary " + primaryOwner +
                     " and backup " + backupOwner);
            }
         }
      }
   }

   public void testLifespanExpiredOnBoth() {
      Object key = createKey(cache0, cache1);
      cache0.put(key, key.toString(), 10, TimeUnit.MINUTES);

      assertEquals(key.toString(), cache0.get(key));
      assertEquals(key.toString(), cache1.get(key));

      // Now we expire on cache0, it should still exist on cache1
      ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
      ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

      // Both should be null
      assertNull(cache0.get(key));
      assertNull(cache1.get(key));
   }

   private void incrementAllTimeServices(long time, TimeUnit unit) {
      for (ControlledTimeService cts : Arrays.asList(ts0, ts1, ts2)) {
         cts.advance(unit.toMillis(time));
      }
   }

   @Test(groups = "unstable", description = "https://issues.redhat.com/browse/ISPN-11422")
   public void testWriteExpiredEntry() {
      String key = "key";
      String value = "value";

      for (int i = 0; i < 100; ++i) {
         Cache<Object, Object> cache = cache0;
         Object prev = cache.get(key);
         if (prev == null) {
            prev = cache.putIfAbsent(key, value, 1, TimeUnit.SECONDS);

            // Should be guaranteed to be null
            assertNull(prev);

            // We should always have a value still
            assertNotNull(cache.get(key));
         }

         long secondOneMilliAdvanced = TimeUnit.SECONDS.toMillis(1);

         ts0.advance(secondOneMilliAdvanced);
         ts1.advance(secondOneMilliAdvanced);
         ts2.advance(secondOneMilliAdvanced);
      }
   }

   // Simpler test for https://issues.redhat.com/browse/ISPN-11422
//   public void testBackupExpirationWritePrimary() {
//      testExpirationButOnBackupDuringWrite(true);
//   }
//
//   public void testBackupExpirationWriteBackup() {
//      testExpirationButOnBackupDuringWrite(false);
//   }
//
//   private void testExpirationButOnBackupDuringWrite(boolean primary) {
//      Object key = createKey(cache0, cache1);
//      String value = key.toString();
//      assertNull(cache0.put(key, value, 10, TimeUnit.SECONDS));
//
//      // Advance the backup so it is expired there
//      ts1.advance(TimeUnit.SECONDS.toMillis(11));
//
//      assertEquals(value, ((primary ? cache0 : cache1).put(key, "replacement-value")));
//   }

   public void testPrimaryNotExpiredButBackupWas() throws InterruptedException, ExecutionException, TimeoutException {
      if (transactional) {
         throw new SkipException("Test isn't supported in transactional mode");
      }
      Object key = createKey(cache0, cache1);
      String value = key.toString();
      cache0.put(key, value,10, TimeUnit.SECONDS);

      final ControlledRpcManager controlledRpcManager = ControlledRpcManager.replaceRpcManager(cache0);
      Class<? extends ReplicableCommand> commandToExpect;
      if (cacheMode == CacheMode.DIST_SYNC) {
         controlledRpcManager.excludeCommands(PutKeyValueCommand.class);
         commandToExpect = BackupWriteCommand.class;
      } else {
         commandToExpect = PutKeyValueCommand.class;
      }

      try {
         Future<Object> result = fork(() -> cache0.put(key, value + "-expire-backup"));

         ControlledRpcManager.BlockedRequest<? extends ReplicableCommand> blockedRequest = controlledRpcManager.expectCommand(commandToExpect);

         incrementAllTimeServices(11, TimeUnit.SECONDS);

         ControlledRpcManager.SentRequest sentRequest = blockedRequest.send();

         if (sentRequest != null) {
            sentRequest.expectAllResponses().receive();
         }

         assertEquals(value, result.get(10, TimeUnit.SECONDS));
      } finally {
         controlledRpcManager.revertRpcManager();
      }
      assertEquals(value + "-expire-backup", cache0.get(key));
      assertEquals(value + "-expire-backup", cache1.get(key));
      assertEquals(value + "-expire-backup", cache2.get(key));
   }

   public void testExpirationWithNoValueEquals() {
      Object key = createKey(cache0, cache1);
      cache0.put(key, new ExpirationFunctionalTest.NoEquals("value"), 10, TimeUnit.MINUTES);

      assertEquals(1, cache0.getAdvancedCache().getDataContainer().sizeIncludingExpired());
      assertEquals(1, cache1.getAdvancedCache().getDataContainer().sizeIncludingExpired());

      // Now we expire on cache0, it should still exist on cache1
      ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
      ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

      assertEquals(1, cache0.getAdvancedCache().getDataContainer().sizeIncludingExpired());
      assertEquals(1, cache1.getAdvancedCache().getDataContainer().sizeIncludingExpired());

      cache0.getAdvancedCache().getExpirationManager().processExpiration();

      verifyNoValue(cache0.getAdvancedCache().getDataContainer().iteratorIncludingExpired());
      verifyNoValue(cache1.getAdvancedCache().getDataContainer().iteratorIncludingExpired());
   }

   private void verifyNoValue(Iterator<InternalCacheEntry<Object, Object>> iter) {
      if (iter.hasNext()) {
         assertNull(iter.next().getValue());
      }
      assertFalse(iter.hasNext());
   }
}
