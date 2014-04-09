package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests data loss during state transfer when the originator of a put operation becomes the primary owner of the
 * modified key. See https://issues.jboss.org/browse/ISPN-3357
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxPutIfAbsentDuringLeaveStressTest")
@CleanupAfterMethod
public class NonTxPutIfAbsentDuringLeaveStressTest extends MultipleCacheManagersTest {

   private static final int NUM_WRITERS = 4;
   private static final int NUM_ORIGINATORS = 2;
   private static final int NUM_KEYS = 100;
   private final ConcurrentMap<String, String> insertedValues = CollectionFactory.makeConcurrentMap();
   private volatile boolean stop = false;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      return c;
   }

   @Test(groups = "unstable")
   public void testNodeLeavingDuringPutIfAbsent() throws Exception {
      Future[] futures = new Future[NUM_WRITERS];
      for (int i = 0; i < NUM_WRITERS; i++) {
         final int writerIndex = i;
         futures[i] = fork(new Callable() {
            @Override
            public Object call() throws Exception {
               while (!stop) {
                  for (int j = 0; j < NUM_KEYS; j++) {
                     Cache<Object, Object> cache = cache(writerIndex % NUM_ORIGINATORS);
                     putRetryOnSuspect(cache, "key_" + j, "value_" + j + "_" + writerIndex);
                  }
               }
               return null;
            }

            private void putRetryOnSuspect(Cache<Object, Object> cache, String key, String value) {
               try {
                  Object oldValue = cache.putIfAbsent(key, value);
                  Object newValue = cache.get(key);
                  if (oldValue == null) {
                     // succeeded
                     log.tracef("Successfully inserted value %s for key %s", value, key);
                     assertEquals(value, newValue);
                     assertNull(insertedValues.putIfAbsent(key, value));
                  } else {
                     // failed
                     assertEquals(oldValue, newValue);
                  }
               } catch (CacheException e) {
                  Throwable ce = e;
                  while (ce instanceof RemoteException) {
                     ce = ce.getCause();
                  }
                  // Retry on OutdatedTopologyException and SuspectException, rethrow any other exception
                  if (!(ce instanceof OutdatedTopologyException) && !(ce instanceof SuspectException))
                     throw e;

                  putRetryOnSuspect(cache, key, value);
               }
            }
         });
      }

      killMember(4);
      TestingUtil.waitForRehashToComplete(caches());

      killMember(3);
      TestingUtil.waitForRehashToComplete(caches());

      stop = true;

      for (int i = 0; i < NUM_WRITERS; i++) {
         futures[i].get(10, TimeUnit.SECONDS);
         for (int j = 0; j < NUM_KEYS; j++) {
            for (int k = 0; k < caches().size(); k++) {
               String key = "key_" + j;
               assertEquals(insertedValues.get(key), cache(k).get(key));
            }
         }
      }
   }
}
