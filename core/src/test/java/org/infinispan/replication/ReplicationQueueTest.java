package org.infinispan.replication;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.concurrent.CountDownLatch;

/**
 * Tests ReplicationQueue's functionality.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "replication.ReplicationQueueTest")
public class ReplicationQueueTest extends MultipleCacheManagersTest {

   private static final int REPL_QUEUE_INTERVAL = 5000;
   private static final int REPL_QUEUE_MAX_ELEMENTS = 10;
   private Cache<Object, Object> cache1;
   private Cache<Object, Object> cache2;

   protected void createCacheManagers() throws Throwable {
      CacheContainer first = TestCacheManagerFactory.createClusteredCacheManager(createGlobalConfigurationBuilder(), new ConfigurationBuilder());
      CacheContainer second = TestCacheManagerFactory.createClusteredCacheManager(createGlobalConfigurationBuilder(), new ConfigurationBuilder());
      registerCacheManager(first, second);

      manager(0).defineConfiguration("replQueue", createCacheConfig(true));
      manager(1).defineConfiguration("replQueue", createCacheConfig(false));

      cache1 = cache(0, "replQueue");
      cache2 = cache(1, "replQueue");
   }

   private GlobalConfigurationBuilder createGlobalConfigurationBuilder() {
      return GlobalConfigurationBuilder.defaultClusteredBuilder();
   }

   private Configuration createCacheConfig(boolean useReplQueue) {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.REPL_ASYNC, true);
      config.clustering()
            .async().useReplQueue(useReplQueue)
            .replQueueInterval(REPL_QUEUE_INTERVAL)
            .replQueueMaxElements(REPL_QUEUE_MAX_ELEMENTS);
      return config.build();
   }

   /**
    * Make sure that replication will occur even if <tt>replQueueMaxElements</tt> are not reached, but the
    * <tt>replQueueInterval</tt> is reached.
    */
   public void testReplicationBasedOnTime() throws Exception {
      //only place one element, queue size is 10.
      cache1.put("key", "value");
      ReplicationQueue replicationQueue = TestingUtil.extractComponent(cache1, ReplicationQueue.class);
      assert replicationQueue != null;
      assert replicationQueue.getElementsCount() == 1;
      assert cache2.get("key") == null;
      assert cache1.get("key").equals("value");

      replicationQueue.flush();

      //in next 5 secs, expect the replication to occur
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 5000) {
         if (cache2.get("key") != null) break;
         Thread.sleep(50);
      }
      assert cache2.get("key").equals("value");
      assert replicationQueue.getElementsCount() == 0;
   }

   /**
    * Make sure that replication will occur even if <tt>replQueueMaxElements</tt> are not reached, but the
    * <tt>replQueueInterval</tt> is reached.
    */
   public void testReplicationBasedOnTimeWithTx() throws Exception {
      //only place one element, queue size is 10.
      TransactionManager transactionManager = TestingUtil.getTransactionManager(cache1);
      transactionManager.begin();
      cache1.put("key", "value");
      transactionManager.commit();

      ReplicationQueue replicationQueue = TestingUtil.extractComponent(cache1, ReplicationQueue.class);
      assert replicationQueue != null;
      assert replicationQueue.getElementsCount() == 1;
      assert cache2.get("key") == null;
      assert cache1.get("key").equals("value");

      replicationQueue.flush();

      //in next 5 secs, expect the replication to occur
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 5000) {
         if (cache2.get("key") != null) break;
         Thread.sleep(50);
      }
      assert cache2.get("key").equals("value");
      assert replicationQueue.getElementsCount() == 0;
   }


   /**
    * Make sure that replication will occur even if <tt>replQueueMaxElements</tt> is reached, but the
    * <tt>replQueueInterval</tt> is not reached.
    */
   public void testReplicationBasedOnSize() throws Exception {
      //place 10 elements, queue size is 10.
      for (int i = 0; i < REPL_QUEUE_MAX_ELEMENTS; i++) {
         cache1.put("key" + i, "value" + i);
      }
      //expect that in next 3 secs all commands are replicated
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 3000) {
         if (cache2.size() == REPL_QUEUE_MAX_ELEMENTS) break;
         Thread.sleep(50);
      }
      for (int i = 0; i < REPL_QUEUE_MAX_ELEMENTS; i++) {
         assert cache2.get("key" + i).equals("value" + i);
      }
   }

   /**
    * Make sure that replication will occur even if <tt>replQueueMaxElements</tt> is reached, but the
    * <tt>replQueueInterval</tt> is not reached.
    */
   public void testReplicationBasedOnSizeWithTx() throws Exception {
      //only place one element, queue size is 10.
      TransactionManager transactionManager = TestingUtil.getTransactionManager(cache1);
      for (int i = 0; i < REPL_QUEUE_MAX_ELEMENTS; i++) {
         transactionManager.begin();
         cache1.put("key" + i, "value" + i);
         transactionManager.commit();
      }
      //expect that in next 3 secs all commands are replicated
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 3000) {
         if (cache2.size() == REPL_QUEUE_MAX_ELEMENTS) break;
         Thread.sleep(50);
      }
      for (int i = 0; i < REPL_QUEUE_MAX_ELEMENTS; i++) {
         assert cache2.get("key" + i).equals("value" + i);
      }
   }

   /**
    * Test that replication queue works fine when multiple threads are putting into the queue.
    */
   public void testReplicationQueueMultipleThreads() throws Exception {
      // put 10 elements in the queue from 5 different threads
      int numThreads = 5;
      final int numLoopsPerThread = 2;
      Thread[] threads = new Thread[numThreads];
      final CountDownLatch latch = new CountDownLatch(1);

      for (int i = 0; i < numThreads; i++) {
         final int i1 = i;
         threads[i] = new Thread() {
            int index = i1;

            public void run() {
               try {
                  latch.await();
               }
               catch (InterruptedException e) {
                  // do nothing
               }
               for (int j = 0; j < numLoopsPerThread; j++) {
                  cache1.put("key" + index + "_" + j, "value");
               }
            }
         };
         threads[i].start();
      }
      latch.countDown();
      // wait for threads to join
      for (Thread t : threads) t.join();

      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 3000) {
         if (cache2.size() == REPL_QUEUE_MAX_ELEMENTS) break;
         Thread.sleep(50);
      }
      assert cache2.size() == REPL_QUEUE_MAX_ELEMENTS;
      ReplicationQueue replicationQueue = TestingUtil.extractComponent(cache1, ReplicationQueue.class);
      assert replicationQueue.getElementsCount() == numThreads * numLoopsPerThread - REPL_QUEUE_MAX_ELEMENTS;
   }

   public void testAtomicHashMap() throws Exception {
      TransactionManager transactionManager = TestingUtil.getTransactionManager(cache1);
      transactionManager.begin();
      AtomicMap am = AtomicMapLookup.getAtomicMap(cache1, "foo");
      am.put("sub-key", "sub-value");
      transactionManager.commit();

      ReplicationQueue replicationQueue = TestingUtil.extractComponent(cache1, ReplicationQueue.class);
      replicationQueue.flush();

      //in next 5 secs, expect the replication to occur
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 5000) {
         if (cache2.get("foo") != null) break;
         Thread.sleep(50);
      }

      assert AtomicMapLookup.getAtomicMap(cache2, "foo", false) != null;
      assert AtomicMapLookup.getAtomicMap(cache2, "foo").get("sub-key") != null;
      assert AtomicMapLookup.getAtomicMap(cache2, "foo").get("sub-key").equals("sub-value");
   }

}
