package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.CacheContainer;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Converter;
import org.infinispan.notifications.KeyValueFilter;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.notifications.cachelistener.filter.SimpleCollectionKeyFilter;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.TransactionMode;
import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * Base class to be used for cluster listener tests for both tx and nontx caches
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional")
public abstract class AbstractClusterListenerTest extends MultipleCacheManagersTest {
   protected final static String CACHE_NAME = "cluster-listener";
   protected final static String FIRST_VALUE = "first-value";
   protected final static String SECOND_VALUE = "second-value";

   protected ConfigurationBuilder builderUsed;
   protected final boolean tx;
   protected final CacheMode cacheMode;

   protected AbstractClusterListenerTest(boolean tx, CacheMode cacheMode) {
      // Have to have cleanup after each method since listeners need to be cleaned up
      cleanup = CleanupPhase.AFTER_METHOD;
      this.tx = tx;
      this.cacheMode = cacheMode;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      createClusteredCaches(3, CACHE_NAME, builderUsed);
   }

   @Listener(clustered = true)
   protected class ClusterListener {
      List<CacheEntryEvent> events = Collections.synchronizedList(new ArrayList<CacheEntryEvent>());

      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      public void onCacheEvent(CacheEntryEvent event) {
         log.debugf("Adding new cluster event %s", event);
         events.add(event);
      }
   }

   protected static class LifespanFilter<K, V> implements KeyValueFilter<K, V>, Serializable {
      public LifespanFilter(long lifespan) {
         this.lifespan = lifespan;
      }

      private final long lifespan;

      @Override
      public boolean accept(K key, V value, Metadata metadata) {
         if (metadata == null) {
            return false;
         }
         // Only accept entities with a lifespan longer than ours
         return metadata.lifespan() > lifespan;
      }
   }

   private static class LifespanConverter implements Converter<Object, String, Object>, Serializable {
      public LifespanConverter(boolean returnOriginalValueOrNull, long lifespanThreshold) {
         this.returnOriginalValueOrNull = returnOriginalValueOrNull;
         this.lifespanThreshold = lifespanThreshold;
      }

      private final boolean returnOriginalValueOrNull;
      private final long lifespanThreshold;

      @Override
      public Object convert(Object key, String value, Metadata metadata) {
         if (metadata != null) {
            long metaLifespan = metadata.lifespan();
            if (metaLifespan > lifespanThreshold) {
               return metaLifespan;
            }
         }
         if (returnOriginalValueOrNull) {
            return value;
         }
         return null;
      }
   }

   protected static class StringTruncator implements Converter<Object, String, String>, Serializable {
      private final int beginning;
      private final int length;

      public StringTruncator(int beginning, int length) {
         this.beginning = beginning;
         this.length = length;
      }

      @Override
      public String convert(Object key, String value, Metadata metadata) {
         if (value != null && value.length() > beginning + length) {
            return value.substring(beginning, beginning + length);
         } else {
            return value;
         }
      }
   }

   @Test
   public void testCreateFromNonOwnerWithListenerNotOwner() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key = new MagicKey(cache1, cache2);
      verifySimpleInsertion(cache2, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   @Test
   public void testCreateFromNonOwnerWithListenerAsBackupOwner() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key = new MagicKey(cache1, cache0);

      verifySimpleInsertion(cache2, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   @Test
   public void testLocalNodeOwnerAndClusterListener() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key = new MagicKey(cache0);

      verifySimpleInsertion(cache0, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   @Test
   public void testLocalNodeNonOwnerAndClusterListener() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      MagicKey key = new MagicKey(cache1, cache2);

      verifySimpleInsertion(cache0, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   @Test
   public void testSimpleFilterNotOwner() {
      testSimpleFilter(new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)));
   }

   @Test
   public void testSimpleFilterLocalOnly() {
      testSimpleFilter(new MagicKey(cache(0, CACHE_NAME)));
   }

   @Test
   public void testMetadataFilterNotOwner() {
      final String keyToFilterOut = "filter-me";
      testFilter(keyToFilterOut, new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)), 1000l, new LifespanFilter<Object, String>(100));
   }

   @Test
   public void testMetadataFilterLocalOnly() {
      final String keyToFilterOut = "filter-me";
      testFilter(keyToFilterOut, new MagicKey(cache(0, CACHE_NAME)), 1000l, new LifespanFilter<Object, String>(100));
   }

   protected void testSimpleFilter(Object key) {
      final String keyToFilterOut = "filter-me";
      testFilter(keyToFilterOut, key, null, new KeyFilterAsKeyValueFilter<Object, String>(
            new SimpleCollectionKeyFilter(Collections.singleton(key))));
   }

   protected void testFilter(Object keyToFilterOut, Object keyToUse, Long lifespan, KeyValueFilter<? super Object, ? super String> filter) {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener, filter, null);

      cache0.put(keyToFilterOut, FIRST_VALUE);

      // We should not have gotten the message since it was filtered
      assertEquals(clusterListener.events.size(), 0);

      verifySimpleInsertion(cache0, keyToUse, FIRST_VALUE, lifespan, clusterListener, FIRST_VALUE);
   }

   @Test
   public void testSimpleConverterNotOwner() {
      testSimpleConverter(new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)));
   }

   @Test
   public void testSimpleConverterLocalOnly() {
      testSimpleConverter(new MagicKey(cache(0, CACHE_NAME)));
   }

   @Test
   public void testMetadataConverterSuccessNotOwner() {
      long lifespan = 25000;
      LifespanConverter converter = new LifespanConverter(true, 500);
      testConverter(new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)), FIRST_VALUE, lifespan, lifespan, converter);
   }

   @Test
   public void testMetadataConverterSuccessLocalOnly() {
      long lifespan = 25000;
      LifespanConverter converter = new LifespanConverter(true, 500);
      testConverter(new MagicKey(cache(0, CACHE_NAME)), FIRST_VALUE, lifespan, lifespan, converter);
   }

   @Test
   public void testMetadataConverterNoPassReturnOriginalNotOwner() {
      long lifespan = 25000;
      LifespanConverter converter = new LifespanConverter(true, Long.MAX_VALUE);
      testConverter(new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)), FIRST_VALUE, FIRST_VALUE, lifespan, converter);
   }

   @Test
   public void testMetadataConverterNoPassReturnOriginalLocalOnly() {
      long lifespan = 25000;
      LifespanConverter converter = new LifespanConverter(true, Long.MAX_VALUE);
      testConverter(new MagicKey(cache(0, CACHE_NAME)), FIRST_VALUE, FIRST_VALUE, lifespan, converter);
   }

   @Test
   public void testMetadataConverterNoPassReturnNullNotOwner() {
      long lifespan = 25000;
      LifespanConverter converter = new LifespanConverter(false, Long.MAX_VALUE);
      testConverter(new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)), FIRST_VALUE, null, lifespan, converter);
   }

   @Test
   public void testMetadataConverterNoPassReturnNullLocalOnly() {
      long lifespan = 25000;
      LifespanConverter converter = new LifespanConverter(false, Long.MAX_VALUE);
      testConverter(new MagicKey(cache(0, CACHE_NAME)), FIRST_VALUE, null, lifespan, converter);
   }

   protected void testSimpleConverter(Object key) {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener, null, new StringTruncator(0, 2));

      verifySimpleInsertion(cache0, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE.substring(0, 2));
   }

   protected void testConverter(Object key, String value, Object resultingValue, Long lifespan, Converter<?, ? super String, ?> converter) {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener, null, converter);

      verifySimpleInsertion(cache0, key, value, lifespan, clusterListener, resultingValue);
   }

   @Test
   public void testClusterListenerNodeGoesDown() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      int cache1ListenerSize = cache1.getAdvancedCache().getListeners().size();
      int cache2ListenerSize = cache2.getAdvancedCache().getListeners().size();

      log.info("Killing node 0 ..");
      TestingUtil.killCacheManagers(manager(0));
      cacheManagers.remove(0);
      log.info("Node 0 killed");

      TestingUtil.blockUntilViewsReceived(60000, false, cache1, cache2);
      TestingUtil.waitForRehashToComplete(cache1, cache2);

      assertEquals(cache1.getAdvancedCache().getListeners().size(), cache1ListenerSize - 1);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), cache2ListenerSize - 1);
   }

   @Test
   public void testNodeComesUpWithClusterListenerAlreadyInstalled() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      Cache<Object, String> cache3 = cache(3, CACHE_NAME);
      MagicKey key = new MagicKey(cache3);

      verifySimpleInsertion(cache3, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   @Test
   public void testNodeComesUpWithClusterListenerAlreadyInstalledFilterAndConverter() {
      final String keyToFilter = "filter-me";
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener, new KeyFilterAsKeyValueFilter<Object, String>(
            new SimpleCollectionKeyFilter<Object>(Collections.singleton(keyToFilter))), new StringTruncator(0, 3));

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      Cache<Object, String> cache3 = cache(3, CACHE_NAME);
      MagicKey key = new MagicKey(cache3);
      cache3.put(key, FIRST_VALUE);

      // Should be filtered
      assertEquals(clusterListener.events.size(), 0);

      verifySimpleInsertion(cache3, keyToFilter, FIRST_VALUE, null, clusterListener, FIRST_VALUE.substring(0, 3));
   }

   @Test
   public void testSimpleClusterListenerRemoved() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      int initialCache0ListenerSize = cache0.getAdvancedCache().getListeners().size();
      int initialCache1ListenerSize = cache1.getAdvancedCache().getListeners().size();
      int initialCache2ListenerSize = cache2.getAdvancedCache().getListeners().size();

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      // Adding a cluster listener should add to each node in cluster
      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 1);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize + 1);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize + 1);

      cache0.removeListener(clusterListener);

      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize);
   }

   @Test
   public void testClusterListenerRemovedWithMultipleInstalledOnSameNode() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      int initialCache0ListenerSize = cache0.getAdvancedCache().getListeners().size();
      int initialCache1ListenerSize = cache1.getAdvancedCache().getListeners().size();
      int initialCache2ListenerSize = cache2.getAdvancedCache().getListeners().size();

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      // Adding a cluster listener should add to each node in cluster
      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 1);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize + 1);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize + 1);

      ClusterListener clusterListener2 = new ClusterListener();
      cache0.addListener(clusterListener2);

      // Adding a second cluster listener should add to each node in cluster as well
      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 2);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize + 2);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize + 2);

      MagicKey key = new MagicKey(cache2, cache1);
      cache1.put(key, FIRST_VALUE);

      // Both listeners should have been notified
      assertEquals(clusterListener.events.size(), 1);
      assertEquals(clusterListener2.events.size(), 1);

      verifySimpleInsertionEvents(clusterListener, key, FIRST_VALUE);
      verifySimpleInsertionEvents(clusterListener2, key, FIRST_VALUE);

      cache0.removeListener(clusterListener);

      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 1);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize + 1);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize + 1);

      // Change the value again to make sure other listener is still working properly
      cache2.put(key, SECOND_VALUE);

      assertEquals(clusterListener2.events.size(), 2);

      CacheEntryEvent event = clusterListener2.events.get(1);

      assertEquals(Event.Type.CACHE_ENTRY_MODIFIED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(SECOND_VALUE, ((CacheEntryModifiedEvent)event).getValue());
   }

   protected void verifySimpleInsertion(Cache<Object, String> cache, Object key, String value, Long lifespan,
                                      ClusterListener listener, Object expectedValue) {
      if (lifespan != null) {
         cache.put(key, FIRST_VALUE, lifespan, TimeUnit.MILLISECONDS);
      } else {
         cache.put(key, FIRST_VALUE);
      }
      verifySimpleInsertionEvents(listener, key, expectedValue);
   }

   protected void verifySimpleInsertionEvents(ClusterListener listener, Object key, Object expectedValue) {
      assertEquals(listener.events.size(), 1);
      CacheEntryEvent event = listener.events.get(0);

      assertEquals(Event.Type.CACHE_ENTRY_CREATED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(expectedValue, event.getValue());
   }

   /**
    * Tests to make sure that if a new node is joining and the node it requested
    * @throws TimeoutException
    * @throws InterruptedException
    * @throws ExecutionException
    */
   @Test
   public void testNodeJoiningAndStateNodeDiesWithExistingClusterListener() throws TimeoutException,
                                                                                   InterruptedException,
                                                                                   ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      int initialCache0ListenerSize = cache0.getAdvancedCache().getListeners().size();
      int initialCache1ListenerSize = cache1.getAdvancedCache().getListeners().size();
      int initialCache2ListenerSize = cache2.getAdvancedCache().getListeners().size();

      ClusterListener clusterListener = new ClusterListener();
      cache2.addListener(clusterListener);

      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 1);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize + 1);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize + 1);

      assertEquals(manager(0).getAddress(), manager(0).getMembers().get(0));

      CheckPoint checkPoint = new CheckPoint();

      waitUntilRequestingListeners(cache0, checkPoint);
      checkPoint.triggerForever("post_cluster_listeners_release_" + cache0);

      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      Future<Cache<Object, String>> future = fork(new Callable<Cache<Object, String>>() {
         @Override
         public Cache<Object, String> call() throws Exception {
            return cache(3, CACHE_NAME);
         }
      });

      checkPoint.awaitStrict("pre_cluster_listeners_invoked_" + cache0, 10, TimeUnit.SECONDS);

      log.info("Killing node 0 ..");
      // Notice we are killing the manager that doesn't have a cache with the cluster listener
      TestingUtil.killCacheManagers(manager(0));
      cacheManagers.remove(0);
      log.info("Node 0 killed");

      waitForClusterToForm(CACHE_NAME);

      checkPoint.triggerForever("pre_cluster_listeners_invoked_" + cache0);

      Cache<Object, String> cache3 = future.get(10, TimeUnit.SECONDS);

      MagicKey key = new MagicKey(cache3);

      verifySimpleInsertion(cache3, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   /**
    * The premise is the same as {@link AbstractClusterListenerTest#testNodeJoiningAndStateNodeDiesWithExistingClusterListener}.
    * This also has the twist of the fact that the node who dies is also has the cluster listener.  This test makes sure
    * that the subsequent node asked for cluster listeners hasn't yet got the view change and still has the cluster
    * listener in it.  Also the requesting node should have the view change before installing.
    * @throws TimeoutException
    * @throws InterruptedException
    * @throws ExecutionException
    */
   @Test(enabled = false, description = "Test may not be doable, check TODO in test")
   public void testNodeJoiningAndStateNodeDiesWhichHasClusterListener() throws TimeoutException,
                                                                                   InterruptedException,
                                                                                   ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      int initialCache0ListenerSize = cache0.getAdvancedCache().getListeners().size();
      int initialCache1ListenerSize = cache1.getAdvancedCache().getListeners().size();
      int initialCache2ListenerSize = cache2.getAdvancedCache().getListeners().size();

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 1);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize + 1);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize + 1);

      // Make sure cache0 will  be the one will get the cluster listener request
      assertEquals(manager(0).getAddress(), manager(0).getMembers().get(0));

      CheckPoint checkPoint = new CheckPoint();

      waitUntilRequestingListeners(cache0, checkPoint);
      checkPoint.triggerForever("post_cluster_listeners_release_" + cache0);
      waitUntilViewChangeOccurs(manager(1), "manager1", checkPoint);

      // We let the first view change occur just fine on cache1 (this will be the addition of cache3).
      // What we want to block is the second one which is the removal of cache0
      checkPoint.trigger("pre_view_listener_release_" + "manager1");

      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      waitUntilViewChangeOccurs(manager(3), "manager3", checkPoint);
      // We don't want to block the view listener change on cache3
      checkPoint.trigger("pre_view_listener_release_" + "manager3");

      Future<Cache<Object, String>> future = fork(new Callable<Cache<Object, String>>() {
         @Override
         public Cache<Object, String> call() throws Exception {
            return cache(3, CACHE_NAME);
         }
      });

      // Wait for view change to occur on cache1 for the addition of cache3
      // Note we haven't triggered the view change for cache1 for the following removal yet
      checkPoint.awaitStrict("post_view_listener_invoked_" + "manager1", 10, TimeUnit.SECONDS);
      // Wait for the cluster listener request to come into cache0 which is from cache3
      checkPoint.awaitStrict("pre_cluster_listeners_invoked_" + cache0, 10, TimeUnit.SECONDS);

      // Now we kill cache0 while it is processing the request from cache3, which will in turn force it to ask cache1
      log.info("Killing node 0 ..");
      TestingUtil.killCacheManagers(manager(0));
      cacheManagers.remove(0);
      log.info("Node 0 killed");

      // TODO: need a away to verify that the response was sent back to cache3 before releasing the next line.  However
      // with no reference to cache I don't think this is possible.  If this can be fixed then test can be reenabled
      Cache<Object, String> cache3 = future.get(10, TimeUnit.SECONDS);

      // Now we can finally let the view change complete on cache1
      checkPoint.triggerForever("pre_view_listener_release_" + "manager1");

      // Now wait for cache3 to come up fully
      waitForClusterToForm(CACHE_NAME);

      MagicKey key = new MagicKey(cache3);
      cache3.put(key, FIRST_VALUE);

      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize);

      // Since we can't get a reliable start count, make sure no RemoteClusterListener is present which is added for
      // a cluster listener
      for (Object listener : cache3.getAdvancedCache().getListeners()) {
         assertFalse(listener instanceof RemoteClusterListener);
      }
   }

   /**
    * This test is to verify then when a new node joins and a cluster listener is installed after the cluster listener
    * request is finished that it finds it.
    *
    * Node 1, 2 & 3 exists
    * Node 2 adds cluster listener - knows to send to listener to Node 1
    * Node 4 starts up
    * Node 4 asks Node 1 for listeners (gets none)
    * Node 1 receives Node 2 listener
    *
    * Test needs to verify in this case that Nod3 3 gets the listener from Node 2
    */
   @Test
   public void testMemberJoinsWhileClusterListenerInstalled() throws TimeoutException, InterruptedException,
                                                                     ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      final Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      CheckPoint checkPoint = new CheckPoint();
      waitUntilListenerInstalled(cache0, checkPoint);
      // We don't want this blocking
      checkPoint.triggerForever("post_add_listener_release_" + cache0);

      final ClusterListener clusterListener = new ClusterListener();
      Future<Void> future = fork(new Callable<Void>() {

         @Override
         public Void call() throws Exception {

            cache1.addListener(clusterListener);
            return null;
         }
      });

      // Now wait until the listener is about to be installed on cache1
      checkPoint.awaitStrict("pre_add_listener_invoked_" + cache0, 10, TimeUnit.SECONDS);

      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      // Now wait for cache3 to come up fully
      waitForClusterToForm(CACHE_NAME);

      Cache<Object, String> cache3 = cache(3, CACHE_NAME);

      // Finally let the listener be added
      checkPoint.triggerForever("pre_add_listener_release_" + cache0);
      future.get(10, TimeUnit.SECONDS);

      MagicKey key = new MagicKey(cache3);
      verifySimpleInsertion(cache3, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   /**
    * Ths test is very similar to {@link AbstractClusterListenerTest#testMemberJoinsWhileClusterListenerInstalled} except that
    * the listener was retrieved in the initial request and thus would have received 2 callables to install the listener.
    * We need to make sure this doesn't cause 2 listeners to be installed causing duplicate messages.
    */
   @Test
   public void testMemberJoinsWhileClusterListenerInstalledDuplicate() throws TimeoutException, InterruptedException,
                                                                     ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      final Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      CheckPoint checkPoint = new CheckPoint();
      waitUntilListenerInstalled(cache0, checkPoint);
      // We want the listener to be able to be added
      checkPoint.triggerForever("pre_add_listener_release_" + cache0);

      final ClusterListener clusterListener = new ClusterListener();
      Future<Void> future = fork(new Callable<Void>() {

         @Override
         public Void call() throws Exception {

            cache1.addListener(clusterListener);
            return null;
         }
      });

      // Now wait until the listener is about to be installed on cache1
      checkPoint.awaitStrict("post_add_listener_invoked_" + cache0, 10, TimeUnit.SECONDS);

      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      // Now wait for cache3 to come up fully
      waitForClusterToForm(CACHE_NAME);

      Cache<Object, String> cache3 = cache(3, CACHE_NAME);

      // Finally let the listener be added
      checkPoint.triggerForever("post_add_listener_release_" + cache0);
      future.get(10, TimeUnit.SECONDS);

      MagicKey key = new MagicKey(cache3);
      verifySimpleInsertion(cache3, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   /**
    * This test is to make sure that if a new node comes up and requests the current cluster listeners that after
    * that is retrieved before processing the response that the node who has the cluster listener dies that we
    * don't keep the local cluster listener around for no reason
    * <p>
    * This may not be feasible since the cluster listener request is during topology change and
    */
   @Test
   public void testMemberJoinsAndRetrievesClusterListenersButMainListenerNodeDiesBeforeInstalled()
         throws TimeoutException, InterruptedException, ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      final Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      final ClusterListener clusterListener = new ClusterListener();
      cache1.addListener(clusterListener);

      assertEquals(manager(0).getAddress(), manager(0).getMembers().get(0));

      CheckPoint checkPoint = new CheckPoint();
      waitUntilRequestingListeners(cache0, checkPoint);
      checkPoint.triggerForever("pre_cluster_listeners_release_" + cache0);

      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      Future<Cache<Object, String>> future = fork(new Callable<Cache<Object, String>>() {
         @Override
         public Cache<Object, String> call() throws Exception {
            return cache(3, CACHE_NAME);
         }
      });

      checkPoint.awaitStrict("post_cluster_listeners_invoked_" + cache0, 10, TimeUnit.SECONDS);

      log.info("Killing node 1 ..");
      // Notice we are killing the manager that doesn't have a cache with the cluster listener
      TestingUtil.killCacheManagers(manager(1));
      cacheManagers.remove(1);
      log.info("Node 1 killed");

      checkPoint.triggerForever("post_cluster_listeners_release_" + cache0);

      // Now wait for cache3 to come up fully
      waitForClusterToForm(CACHE_NAME);

      Cache<Object, String> cache3 = future.get(10, TimeUnit.SECONDS);

      for (Object listener : cache3.getAdvancedCache().getListeners()) {
         assertFalse(listener instanceof RemoteClusterListener);
      }
   }

   protected void waitUntilListenerInstalled(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      CacheNotifier cn = TestingUtil.extractComponent(cache, CacheNotifier.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(cn);
      CacheNotifier mockNotifier = mock(CacheNotifier.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            // Wait for main thread to sync up
            checkPoint.trigger("pre_add_listener_invoked_" + cache);
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_add_listener_release_" + cache, 10, TimeUnit.SECONDS);

            try {
               return forwardedAnswer.answer(invocation);
            } finally {
               // Wait for main thread to sync up
               checkPoint.trigger("post_add_listener_invoked_" + cache);
               // Now wait until main thread lets us through
               checkPoint.awaitStrict("post_add_listener_release_" + cache, 10, TimeUnit.SECONDS);
            }
         }
      }).when(mockNotifier).addListener(anyObject(), any(KeyValueFilter.class), any(Converter.class));
      TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
   }

   protected void waitUntilNotificationRaised(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      CacheNotifier cn = TestingUtil.extractComponent(cache, CacheNotifier.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(cn);
      CacheNotifier mockNotifier = mock(CacheNotifier.class, withSettings().defaultAnswer(forwardedAnswer));
      Answer answer = new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            // Wait for main thread to sync up
            checkPoint.trigger("pre_raise_notification_invoked");
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_raise_notification_release", 10, TimeUnit.SECONDS);

            try {
               return forwardedAnswer.answer(invocation);
            } finally {
               // Wait for main thread to sync up
               checkPoint.trigger("post_raise_notification_invoked");
               // Now wait until main thread lets us through
               checkPoint.awaitStrict("post_raise_notification_release", 10, TimeUnit.SECONDS);
            }
         }
      };
      doAnswer(answer).when(mockNotifier).notifyCacheEntryCreated(any(), any(), eq(false), any(InvocationContext.class),
                                                                  any(FlagAffectedCommand.class));
      doAnswer(answer).when(mockNotifier).notifyCacheEntryModified(any(), any(), anyBoolean(), eq(false),
                                                                   any(InvocationContext.class),
                                                                   any(FlagAffectedCommand.class));
      doAnswer(answer).when(mockNotifier).notifyCacheEntryRemoved(any(), any(), any(), eq(false),
                                                                  any(InvocationContext.class),
                                                                  any(FlagAffectedCommand.class));
      TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
   }

   protected void waitUntilRequestingListeners(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      StateProvider sp = TestingUtil.extractComponent(cache, StateProvider.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(sp);
      StateProvider mockProvider = mock(StateProvider.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            // Wait for main thread to sync up
            checkPoint.trigger("pre_cluster_listeners_invoked_" + cache);
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_cluster_listeners_release_" + cache, 10, TimeUnit.SECONDS);

            try {
               return forwardedAnswer.answer(invocation);
            } finally {
               // Wait for main thread to sync up
               checkPoint.trigger("post_cluster_listeners_invoked_" + cache);
               // Now wait until main thread lets us through
               checkPoint.awaitStrict("post_cluster_listeners_release_" + cache, 10, TimeUnit.SECONDS);
            }
         }
      }).when(mockProvider).getClusterListenersToInstall();
      TestingUtil.replaceComponent(cache, StateProvider.class, mockProvider, true);
   }

   protected void waitUntilViewChangeOccurs(final CacheContainer cacheContainer, final String uniqueId, final CheckPoint checkPoint) {
      CacheManagerNotifier cmn = TestingUtil.extractGlobalComponent(cacheContainer, CacheManagerNotifier.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(cmn);
      CacheManagerNotifier mockNotifier = mock(CacheManagerNotifier.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            // Wait for main thread to sync up
            checkPoint.trigger("pre_view_listener_invoked_" + uniqueId);
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_view_listener_release_" + uniqueId, 10, TimeUnit.SECONDS);

            try {
               return forwardedAnswer.answer(invocation);
            } finally {
               checkPoint.trigger("post_view_listener_invoked_" + uniqueId);
            }
         }
      }).when(mockNotifier).notifyViewChange(anyList(), anyList(), any(Address.class), anyInt());
      TestingUtil.replaceComponent(cacheContainer, CacheManagerNotifier.class, mockNotifier, true);
   }
}
