package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;

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

   public void testNodeLeavingDuringPutIfAbsent() throws Exception {
      Future[] futures = new Future[NUM_WRITERS];
      for (int i = 0; i < NUM_WRITERS; i++) {
         final int finalI = i;
         futures[i] = fork(new Callable() {
            @Override
            public Object call() throws Exception {
               for (int j = 0; j < NUM_KEYS; j++) {
                  Cache<Object, Object> cache = cache(finalI % NUM_ORIGINATORS);
                  putRetryOnSuspect(cache, "key_" + finalI + "_" + j, "value_" + finalI + "_" + j);
               }
               return null;
            }

            private void putRetryOnSuspect(Cache<Object, Object> cache, String key, String value) {
               try {
                  cache.putIfAbsent(key, value);
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
      waitForClusterToForm();

      killMember(3);
      waitForClusterToForm();

      TimeUnit.MILLISECONDS.sleep(NUM_KEYS * NUM_WRITERS);

      for (int i = 0; i < NUM_WRITERS; i++) {
         futures[i].get(10, TimeUnit.SECONDS);
         for (int j = 0; j < NUM_KEYS; j++) {
            for (int k = 0; k < caches().size(); k++) {
               assertEquals("value_" + i + "_" + j, cache(k).get("key_" + i + "_" + j));
            }
         }
      }
   }
}