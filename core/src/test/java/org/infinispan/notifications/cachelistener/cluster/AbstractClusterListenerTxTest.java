package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import java.util.Collection;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * This is a base class defining tests that are common across transactional tests when dealing with a cluster listener
 *
 * @author wburns
 * @since 4.0
 */
@Test(groups = "functional")
public abstract class AbstractClusterListenerTxTest extends AbstractClusterListenerTest {
   protected AbstractClusterListenerTxTest(CacheMode cacheMode) {
      super(true, cacheMode);
   }

   // Checks the given events to see if an entry was marked as being created for the provided key and value
   // Note this will have to iterate over all the events until one is found, since it is possible to get the events in
   // non-deterministic order since they aren't from the same node
   protected void verifyCreation(Collection<CacheEntryEvent> events, Object key, Object expectedValue) {
      boolean found = false;
      for (CacheEntryEvent event : events) {
         if (Event.Type.CACHE_ENTRY_CREATED == event.getType() && key.equals(event.getKey()) &&
               expectedValue.equals(event.getValue())) {
            found = true;
            break;
         }
      }
      assertTrue(found, "No entry was created in provided events " + events + " matching key " + key + " and value " +
            expectedValue);
   }

   @Test
   public void testBatchedCommitOriginatorNotLocal() throws SystemException, NotSupportedException, HeuristicRollbackException,
                                                  HeuristicMixedException, RollbackException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key1 = new MagicKey(cache1, cache2);
      MagicKey key2 = new MagicKey(cache2, cache1);

      TransactionManager tm = cache2.getAdvancedCache().getTransactionManager();
      tm.begin();

      cache2.put(key1, FIRST_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      cache2.put(key2, SECOND_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      tm.commit();

      assertEquals(clusterListener.events.size(), 2);
      verifyCreation(clusterListener.events, key1, FIRST_VALUE);
      verifyCreation(clusterListener.events, key2, SECOND_VALUE);
   }

   @Test
   public void testBatchedCommitKeyNotLocalLocal() throws HeuristicRollbackException, RollbackException, HeuristicMixedException,
                                               SystemException, NotSupportedException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key1 = new MagicKey(cache1, cache2);
      MagicKey key2 = new MagicKey(cache2, cache1);

      TransactionManager tm = cache2.getAdvancedCache().getTransactionManager();
      tm.begin();

      cache0.put(key1, FIRST_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      cache0.put(key2, SECOND_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      tm.commit();

      assertEquals(clusterListener.events.size(), 2);
      verifyCreation(clusterListener.events, key1, FIRST_VALUE);
      verifyCreation(clusterListener.events, key2, SECOND_VALUE);
   }

   @Test
   public void testBatchedCommitLocal() throws HeuristicRollbackException, RollbackException, HeuristicMixedException,
                                               SystemException, NotSupportedException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key1 = new MagicKey(cache0);
      MagicKey key2 = new MagicKey(cache1, cache0);

      TransactionManager tm = cache2.getAdvancedCache().getTransactionManager();
      tm.begin();

      cache0.put(key1, FIRST_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      cache0.put(key2, SECOND_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      tm.commit();

      assertEquals(clusterListener.events.size(), 2);
      verifyCreation(clusterListener.events, key1, FIRST_VALUE);
      verifyCreation(clusterListener.events, key2, SECOND_VALUE);
   }

   @Test
   public void testRolledBackNotLocal() throws SystemException, NotSupportedException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key1 = new MagicKey(cache1, cache2);
      MagicKey key2 = new MagicKey(cache2, cache1);

      TransactionManager tm = cache2.getAdvancedCache().getTransactionManager();
      tm.begin();

      cache2.put(key1, FIRST_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      cache2.put(key2, SECOND_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      tm.rollback();

      assertEquals(clusterListener.events.size(), 0);
   }

   @Test
   public void testRolledBackOriginatorNotLocal() throws SystemException, NotSupportedException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key1 = new MagicKey(cache0);
      MagicKey key2 = new MagicKey(cache1, cache0);

      TransactionManager tm = cache2.getAdvancedCache().getTransactionManager();
      tm.begin();

      cache2.put(key1, FIRST_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      cache2.put(key2, SECOND_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      tm.rollback();

      assertEquals(clusterListener.events.size(), 0);
   }

   @Test
   public void testRolledBackLocal() throws SystemException, NotSupportedException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key1 = new MagicKey(cache0);
      MagicKey key2 = new MagicKey(cache1, cache0);

      TransactionManager tm = cache2.getAdvancedCache().getTransactionManager();
      tm.begin();

      cache0.put(key1, FIRST_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      cache0.put(key2, SECOND_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      tm.rollback();

      assertEquals(clusterListener.events.size(), 0);
   }

   @Test
   public void testMultipleKeysSameOwnerBatchNotified() throws SystemException, NotSupportedException, HeuristicRollbackException,
                                                          HeuristicMixedException, RollbackException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key1 = new MagicKey(cache1);
      MagicKey key2 = new MagicKey(cache1);

      TransactionManager tm = cache2.getAdvancedCache().getTransactionManager();
      tm.begin();

      cache2.put(key1, FIRST_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      cache2.put(key2, SECOND_VALUE);
      assertEquals(clusterListener.events.size(), 0);

      tm.commit();

      assertEquals(clusterListener.events.size(), 2);
      verifyCreation(clusterListener.events, key1, FIRST_VALUE);
      verifyCreation(clusterListener.events, key2, SECOND_VALUE);
   }
}
