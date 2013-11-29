package org.infinispan.lucene.testutils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

/**
 * CacheFactory useful to create clustered caches on-demand in several tests.
 * The same thread is used to actually create each cache, making it possible to create
 * several connected caches even though the testing suite in Infinispan isolates different threads.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
@ThreadSafe
@SuppressWarnings("unchecked")
public class ClusteredCacheFactory {

   private final BlockingQueue<ConfigurationBuilder> requests = new SynchronousQueue<ConfigurationBuilder>();
   private final BlockingQueue<Cache> results = new SynchronousQueue<Cache>();
   private final ExecutorService executor = Executors.newSingleThreadExecutor();
   private final ConfigurationBuilder cfg;

   @GuardedBy("this") private boolean started = false;
   @GuardedBy("this") private boolean stopped = false;

   /**
    * Create a new ClusteredCacheFactory.
    *
    * @param cfg defines the configuration used to build the caches
    */
   public ClusteredCacheFactory(ConfigurationBuilder cfg) {
      this.cfg = cfg;
   }

   /**
    * Create a cache using default configuration
    * @throws InterruptedException if interrupted while waiting for the cache construction
    */
   public synchronized Cache createClusteredCache() throws InterruptedException {
      if (!started)
         throw new IllegalStateException("was not started");
      if (stopped)
         throw new IllegalStateException("was already stopped");
      requests.put(cfg);
      return results.take();
   }

   public Cache createClusteredCacheWaitingForNodesView(int expectedGroupSize) throws InterruptedException {
      Cache cache = createClusteredCache();
      TestingUtil.blockUntilViewReceived(cache, expectedGroupSize, 10000, false);
      return cache;
   }

   public synchronized void start() {
      if (started)
         throw new IllegalStateException("was already started");
      if (stopped)
         throw new IllegalStateException("was already stopped");
      started = true;
      executor.execute(new Worker());
   }

   public synchronized void stop() {
      if (stopped)
         throw new IllegalStateException("was already stopped");
      if (!started)
         throw new IllegalStateException("was not started");
      stopped = true;
      executor.shutdownNow();
   }

   private class Worker implements Runnable {

      @Override
      public void run() {
         while (true) {
            try {
               ConfigurationBuilder configuration = requests.take();
               CacheContainer cacheContainer = TestCacheManagerFactory.createClusteredCacheManager(configuration);
               Cache cache = cacheContainer.getCache();
               results.put(cache);
            } catch (InterruptedException e) {
               return;
            }
         }
      }

   }

}
