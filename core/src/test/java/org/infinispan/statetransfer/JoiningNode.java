package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

   public <K, V> Cache<K, V> getCache() {
      return cm.getCache();
   }

   public <K, V> Cache<K, V> getCache(String cacheName) {
      return cm.getCache(cacheName);
   }

   public void waitForJoin(long timeout, Cache... caches) throws InterruptedException {
      // Wait for either a merge or view change to happen
      latch.await(timeout, TimeUnit.MILLISECONDS);
      // Wait for the state transfer to end
      TestingUtil.waitForRehashToComplete(caches);
   }

   private boolean isStateTransferred() {
      return !listener.merged;
   }

   void verifyStateTransfer(Callable<Void> verify) throws Exception {
      if (isStateTransferred())
         verify.call();
   }

}
