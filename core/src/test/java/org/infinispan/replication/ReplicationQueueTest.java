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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests ReplicationQueue's functionality.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "replication.ReplicationQueueTest")
public class ReplicationQueueTest extends MultipleCacheManagersTest {

   private static final int REPL_QUEUE_INTERVAL = 1000;
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
      assertNotNull(replicationQueue);
      assertEquals(1, replicationQueue.getElementsCount());
      assertNull(cache2.get("key"));
      assertEquals("value", cache1.get("key"));

      replicationQueue.flush();

      // Now wait until values are replicated properly
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache2.get("key") != null;
         }
      });
      assertEquals(cache2.get("key"), "value");
      assertEquals(0, replicationQueue.getElementsCount());
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
      assertNotNull(replicationQueue);
      assertEquals(replicationQueue.getElementsCount(), 1);
      assertNull(cache2.get("key"));
      assertEquals(cache1.get("key"), "value");

      replicationQueue.flush();

      // Now wait until values are replicated properly
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache2.get("key") != null;
         }
      });
      assertEquals(cache2.get("key"), "value");
      assertEquals(0, replicationQueue.getElementsCount());
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
      // Now wait until values are replicated properly
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache2.size() == REPL_QUEUE_MAX_ELEMENTS;
         }
      });
      for (int i = 0; i < REPL_QUEUE_MAX_ELEMENTS; i++) {
         assertEquals("value" + i, cache2.get("key" + i));
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
      // Now wait until values are replicated properly
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache2.size() == REPL_QUEUE_MAX_ELEMENTS;
         }
      });
      for (int i = 0; i < REPL_QUEUE_MAX_ELEMENTS; i++) {
         assertEquals("value" + i, cache2.get("key" + i));
      }
   }

   /**
    * Test that replication queue works fine when multiple threads are putting into the queue.
    */
   public void testReplicationQueueMultipleThreads() throws Exception {
      // put 12 elements in the queue from 4 different threads
      final int numThreads = 4;
      final int numLoopsPerThread = 3;

      runConcurrently(new Callable() {
         AtomicInteger indexOffset = new AtomicInteger();

         public Void call() {
            int index = indexOffset.getAndIncrement();
            for (int j = 0; j < numLoopsPerThread; j++) {
               cache1.put("key" + index + "_" + j, "value");
            }
            return null;
         }
      }, numThreads);

      // Now wait until values are replicated properly
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache2.size() == numThreads * numLoopsPerThread;
         }
      });
      ReplicationQueue replicationQueue = TestingUtil.extractComponent(cache1, ReplicationQueue.class);
      assertEquals(0, replicationQueue.getElementsCount());
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

      assertNotNull(AtomicMapLookup.getAtomicMap(cache2, "foo", false));
      assertNotNull(AtomicMapLookup.getAtomicMap(cache2, "foo").get("sub-key"));
      assertEquals("sub-value", AtomicMapLookup.getAtomicMap(cache2, "foo").get("sub-key"));
   }

}
