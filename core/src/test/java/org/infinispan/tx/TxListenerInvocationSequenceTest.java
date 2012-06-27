package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.org
 */
@Test(groups = "functional", testName = "tx.TxListenerInvocationSequenceTest")
public class TxListenerInvocationSequenceTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      createClusteredCaches(2, cacheConfig);
      waitForClusterToForm();
   }

   public void testSameInvokingSequence() {
      TxListener l0 = new TxListener(0);
      cache(0).addListener(l0);
      TxListener l1 = new TxListener(1);
      cache(1).addListener(l1);
      cache(0).put("k", "v");

      assertEquals(l0.log, l1.log);
      assertEquals(l0.log,Arrays.asList(TxEvent.STARTED, TxEvent.CREATED, TxEvent.COMPLETED));
   }


   enum TxEvent {
      STARTED, CREATED, COMPLETED
   }

   @Listener
   public static class TxListener {

      int cacheIndex;
      List<TxEvent> log = new ArrayList<TxEvent>(3);


      public TxListener(int cacheIndex) {
         this.cacheIndex = cacheIndex;
      }

      @TransactionRegistered
      public void startTx(TransactionRegisteredEvent tre) {
         if (!tre.isPre()) log.add(TxEvent.STARTED);
      }

      @CacheEntryCreated
      public void entryCreated(CacheEntryCreatedEvent cec) {
         if (cec.isPre()) log.add(TxEvent.CREATED);
      }

      @TransactionCompleted
      public void txCompleted(TransactionCompletedEvent tce) {
         if (!tce.isPre()) log.add(TxEvent.COMPLETED);
      }
   }
}
