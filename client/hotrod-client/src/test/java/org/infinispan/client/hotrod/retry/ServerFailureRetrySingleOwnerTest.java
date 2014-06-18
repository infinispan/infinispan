package org.infinispan.client.hotrod.retry;

import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.*;

/**
 * Tests that force operations to be directed to a server that applies the
 * change locally but fails to send it to other nodes. The failover mechanism
 * in the Hot Rod client will determine a different server and retry.
 */
@Test(groups = "functional", testName = "client.hotrod.retry.ServerFailureRetryTest")
public class ServerFailureRetrySingleOwnerTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numOwners(1).numSegments(1)
            .consistentHashFactory(new ControlledConsistentHashFactory(0))
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      return builder;
   }

   public void testRetryReplaceWithVersion() {
      ErrorInducingListener listener = new ErrorInducingListener();
      byte[] key = TestHelper.getKeyForServer(hotRodServer1);
      try {
         assertNull(remoteCache.putIfAbsent(key, 1));
         VersionedValue versioned = remoteCache.getVersioned(key);
         assertEquals(1, versioned.getValue());
         hotRodServer1.getCacheManager().getCache().addListener(listener);
         assertFalse(listener.errorInduced);
         assertEquals(true, remoteCache.replaceWithVersion(key, 2, versioned.getVersion()));
         assertTrue(listener.errorInduced);
         assertEquals(2, remoteCache.get(key));
      } finally {
         hotRodServer1.getCacheManager().removeListener(listener);
      }
   }

   public void testRetryRemoveWithVersion() {
      ErrorInducingListener listener = new ErrorInducingListener();
      byte[] key = TestHelper.getKeyForServer(hotRodServer1);
      try {
         assertNull(remoteCache.putIfAbsent(key, 1));
         VersionedValue versioned = remoteCache.getVersioned(key);
         assertEquals(1, versioned.getValue());
         hotRodServer1.getCacheManager().getCache().addListener(listener);
         assertFalse(listener.errorInduced);
         assertEquals(true, remoteCache.removeWithVersion(key, versioned.getVersion()));
         assertTrue(listener.errorInduced);
         assertNull(remoteCache.get(key));
      } finally {
         hotRodServer1.getCacheManager().removeListener(listener);
      }
   }

   public void testRetryRemove() {
      ErrorInducingListener listener = new ErrorInducingListener();
      byte[] key = TestHelper.getKeyForServer(hotRodServer1);
      try {
         assertNull(remoteCache.putIfAbsent(key, 1));
         hotRodServer1.getCacheManager().getCache().addListener(listener);
         assertFalse(listener.errorInduced);
         assertEquals(1, remoteCache.remove(key));
         assertTrue(listener.errorInduced);
         assertNull(remoteCache.get(key));
      } finally {
         hotRodServer1.getCacheManager().removeListener(listener);
      }
   }

   public void testRetryReplace() {
      ErrorInducingListener listener = new ErrorInducingListener();
      byte[] key = TestHelper.getKeyForServer(hotRodServer1);
      try {
         assertNull(remoteCache.putIfAbsent(key, 1));
         hotRodServer1.getCacheManager().getCache().addListener(listener);
         assertFalse(listener.errorInduced);
         assertEquals(1, remoteCache.replace(key, 2));
         assertTrue(listener.errorInduced);
         assertEquals(2, remoteCache.get(key));
      } finally {
         hotRodServer1.getCacheManager().removeListener(listener);
      }
   }

   public void testRetryPutIfAbsent() {
      ErrorInducingListener listener = new ErrorInducingListener();
      byte[] key = TestHelper.getKeyForServer(hotRodServer1);
      hotRodServer1.getCacheManager().getCache().addListener(listener);
      try {
         assertFalse(listener.errorInduced);
         assertNull(remoteCache.putIfAbsent(key, 1));
         assertTrue(listener.errorInduced);
         assertEquals(1, remoteCache.get(key));
      } finally {
         hotRodServer1.getCacheManager().removeListener(listener);
      }
   }

   public void testRetryPutOnNonEmpty() {
      ErrorInducingListener listener = new ErrorInducingListener();
      byte[] key = TestHelper.getKeyForServer(hotRodServer1);
      try {
         assertNull(remoteCache.put(key, 1));
         hotRodServer1.getCacheManager().getCache().addListener(listener);
         assertFalse(listener.errorInduced);
         assertEquals(1, remoteCache.put(key, 2));
         assertTrue(listener.errorInduced);
         assertEquals(2, remoteCache.get(key));
      } finally {
         hotRodServer1.getCacheManager().removeListener(listener);
      }
   }

   public void testRetryPutOnEmpty() {
      ErrorInducingListener listener = new ErrorInducingListener();
      byte[] key = TestHelper.getKeyForServer(hotRodServer1);
      hotRodServer1.getCacheManager().getCache().addListener(listener);
      try {
         assertFalse(listener.errorInduced);
         assertNull(remoteCache.put(key, 1));
         assertTrue(listener.errorInduced);
         assertEquals(1, remoteCache.get(key));
      } finally {
         hotRodServer1.getCacheManager().removeListener(listener);
      }
   }

   @Listener
   public static class ErrorInducingListener {
      boolean errorInduced;

      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      @SuppressWarnings("unused")
      public void handleEvent(CacheEntryEvent event) throws Exception {
         if (!event.isPre() && event.isOriginLocal()) {
            errorInduced = true;
            throw new SuspectException("Simulated suspicion");
         }
      }
   }


}
