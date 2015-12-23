package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertTrue;

/**
 * Represents a joining node, designed for state transfer related tests.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class JoiningNode {

   private final EmbeddedCacheManager cm;
   private final CountDownLatch latch;
   private final MergeOrViewChangeListener listener;

   public JoiningNode(EmbeddedCacheManager cm) {
      this.cm = cm;
      latch = new CountDownLatch(1);
      listener = new MergeOrViewChangeListener(latch);
      cm.addListener(listener);
   }

   public Cache getCache() {
      return cm.getCache();
   }

   public Cache getCache(String cacheName) {
      return cm.getCache(cacheName);
   }

   public void waitForJoin(long timeout, Cache... caches) throws InterruptedException {
      // Wait for either a merge or view change to happen
      latch.await(timeout, TimeUnit.MILLISECONDS);
      assertTrue(isStateTransferred());

      // Wait for the state transfer to end
      TestingUtil.waitForRehashToComplete(caches);
   }

   public boolean isStateTransferred() {
      return !listener.merged;
   }
}
