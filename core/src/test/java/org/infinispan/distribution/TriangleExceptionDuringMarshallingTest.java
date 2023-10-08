package org.infinispan.distribution;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.MarshallingExceptionGenerator;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test that ack collectors are closed properly after a marshalling exception.
 *
 * <p>See ISPN-12435</p>
 *
 * @author Dan Berindei
 * @since 12.1
 */
@Test(groups = "unit", testName = "distribution.TriangleExceptionDuringMarshallingTest")
public class TriangleExceptionDuringMarshallingTest extends MultipleCacheManagersTest {
   public static final int NUM_SEGMENTS = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.serialization().marshaller(new JavaSerializationMarshaller());
      globalBuilder.serialization().allowList()
                   .addClasses(ControlledConsistentHashFactory.Default.class, MagicKey.class, MarshallingExceptionGenerator.class);

      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      ControlledConsistentHashFactory<?> chf =
            new ControlledConsistentHashFactory.Default(new int[][]{{0, 1}, {1, 2}, {2, 0}});
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
                  .hash().numSegments(NUM_SEGMENTS).consistentHashFactory(chf);

      createCluster(globalBuilder, cacheBuilder, 3);

      // Make sure we're using the triangle algorithm
      AsyncInterceptorChain asyncInterceptorChain = extractInterceptorChain(cache(0));
      assertTrue(asyncInterceptorChain.containsInterceptorType(TriangleDistributionInterceptor.class));
   }

   public void testExceptionDuringMarshallingOnOriginator() {
      // fail during the first serialization (i.e. on the originator)
      Object value = MarshallingExceptionGenerator.failOnSerialization(0);

      Cache<Object, Object> originCache = cache(0);
      MagicKey primaryKey = new MagicKey("primary", cache(0));
      expectException(MarshallingException.class, () -> originCache.put(primaryKey, value));
      assertCleanFailure(originCache, primaryKey);

      MagicKey nonOwnerKey = new MagicKey("non-owner", cache(1));
      expectException(MarshallingException.class, () -> originCache.put(nonOwnerKey, value));
      assertCleanFailure(originCache, nonOwnerKey);

      MagicKey backupKey = new MagicKey("backup", cache(2));
      expectException(MarshallingException.class, () -> originCache.put(backupKey, value));
      assertCleanFailure(originCache, backupKey);
   }

   public void testExceptionDuringMarshallingOnRemote() {
      // fail during the second serialization, i.e. remotely
      Object value = MarshallingExceptionGenerator.failOnSerialization(1);

      Cache<Object, Object> originCache = cache(0);
      // does not fail when the originator is the primary, as there is no remote serialization
      MagicKey primaryKey = new MagicKey("primary", cache(0));
      originCache.put(primaryKey, value);
      originCache.remove(primaryKey);

      // fails when the originator is not an owner
      MagicKey nonOwnerKey = new MagicKey("non-owner", cache(1));
      expectException(RemoteException.class, MarshallingException.class, () -> originCache.put(nonOwnerKey, value));
      assertCleanFailure(originCache, nonOwnerKey);

      // fails when the originator is a backup
      MagicKey backupKey = new MagicKey("backup", cache(2));
      expectException(RemoteException.class, MarshallingException.class, () -> originCache.put(backupKey, value));
      assertCleanFailure(originCache, backupKey);
   }

   @Test(enabled = false, description = "See ISPN-12770")
   public void testExceptionDuringUnmarshalling() {
      // fail during the second serialization, i.e. remotely
      Object value = MarshallingExceptionGenerator.failOnDeserialization(0);

      Cache<Object, Object> originCache = cache(0);
      MagicKey primaryKey = new MagicKey("primary", cache(0));
      expectException(MarshallingException.class, () -> originCache.put(primaryKey, value));
      assertCleanFailure(originCache, primaryKey);

      MagicKey nonOwnerKey = new MagicKey("non-owner", cache(1));
      expectException(MarshallingException.class, () -> originCache.put(nonOwnerKey, value));
      assertCleanFailure(originCache, nonOwnerKey);

      MagicKey backupKey = new MagicKey("backup", cache(2));
      expectException(MarshallingException.class, () -> originCache.put(backupKey, value));
      assertCleanFailure(originCache, backupKey);
   }

   private void assertCleanFailure(Cache<Object, Object> originCache, MagicKey key) {
      // verify that ack collector it cleaned up and value is not inserted
      assertInvocationIsDone(singleton(key));
      assertCacheIsEmpty();

      // verify that a put and remove with the same key and a marshallable value succeeds
      originCache.put(key, "good_value");
      originCache.remove(key);
   }

   public void testExceptionDuringMarshallingOnOriginatorMultiKey() {
      MarshallingExceptionGenerator value = MarshallingExceptionGenerator.failOnSerialization(0);
      Map<Object, Object> values = new HashMap<>();
      values.put(new MagicKey(cache(0)), value);
      values.put(new MagicKey(cache(1)), value);
      values.put(new MagicKey(cache(2)), value);

      for (Cache<Object, Object> cache : caches()) {
         expectException(MarshallingException.class, () -> cache.putAll(values));
         assertInvocationIsDone(values.keySet());
         assertCacheIsEmpty();
      }
   }

   public void testExceptionDuringMarshallingOnRemoteMultiKey() {
      MarshallingExceptionGenerator value = MarshallingExceptionGenerator.failOnSerialization(1);
      Map<Object, Object> values = new HashMap<>();
      values.put(new MagicKey(cache(0)), value);
      values.put(new MagicKey(cache(1)), value);
      values.put(new MagicKey(cache(2)), value);

      for (Cache<Object, Object> cache : caches()) {
         expectException(RemoteException.class, MarshallingException.class, () -> cache.putAll(values));
         assertInvocationIsDone(values.keySet());

         for (Object key : values.keySet()) {
            cache.remove(key);
         }
      }
   }

   private void assertCacheIsEmpty() {
      for (Cache<Object, Object> cache : caches()) {
         assertEquals(0, cache.getAdvancedCache().getDataContainer().sizeIncludingExpired());
      }
   }

   private void assertInvocationIsDone(Collection<?> keys) {
      for (Cache<Object, Object> cache : caches()) {
         CommandAckCollector ackCollector = extractComponent(cache, CommandAckCollector.class);
         assertEquals(emptyList(), ackCollector.getPendingCommands());

         LockManager lm = TestingUtil.extractLockManager(cache);
         for (Object key : keys) {
            assert !lm.isLocked(key);
         }
      }
   }

   @AfterMethod(alwaysRun = true)
   public void cleanup() {
      LocalizedCacheTopology cacheTopology = extractCacheTopology(cache(0));
      int topologyId = cacheTopology.getTopologyId();

      for (Cache<Object, Object> cache : caches()) {
         // Complete pending commands
         CommandAckCollector ackCollector = extractComponent(cache, CommandAckCollector.class);
         for (Long pendingCommand : ackCollector.getPendingCommands()) {
            ackCollector.completeExceptionally(pendingCommand, new TestException(), topologyId);
         }

         // Release locks
         LockManager lockManager = extractComponent(cache, LockManager.class);
         assertEquals(0, lockManager.getNumberOfLocksHeld());
      }

      // Mark all sequence ids as delivered
      for (int segment = 0; segment < NUM_SEGMENTS; segment++) {
         DistributionInfo segmentDistribution = cacheTopology.getSegmentDistribution(segment);
         Address primary = segmentDistribution.primary();
         Cache<?, ?> primaryCache = manager(primary).getCache();
         long latestSequenceId = extractComponent(primaryCache, TriangleOrderManager.class)
               .latestSent(segment, topologyId);

         for (int i = 0; i <= latestSequenceId; i++) {
            for (Address backup : segmentDistribution.writeBackups()) {
               Cache<Object, Object> backupCache = manager(backup).getCache();
               extractComponent(backupCache, TriangleOrderManager.class)
                     .markDelivered(segment, i, topologyId);
            }
         }
      }
   }

}
