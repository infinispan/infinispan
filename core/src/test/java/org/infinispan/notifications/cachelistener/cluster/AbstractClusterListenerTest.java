package org.infinispan.notifications.cachelistener.cluster;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.KeyFilterAsCacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.KeyValueFilterAsCacheEventFilter;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Base class to be used for cluster listener tests for both tx and nontx caches
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional")
public abstract class AbstractClusterListenerTest extends AbstractClusterListenerUtilTest {
   protected AbstractClusterListenerTest(boolean tx, CacheMode cacheMode) {
      super(tx, cacheMode);
   }

   protected ClusterListener listener() {
      return new ClusterListener();
   }

   @Test
   public void testCreateFromNonOwnerWithListenerNotOwner() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      MagicKey key = new MagicKey(cache1, cache2);
      verifySimpleInsertion(cache2, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   @Test
   public void testCreateFromNonOwnerWithListenerAsBackupOwner() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      MagicKey key = new MagicKey(cache1, cache0);

      verifySimpleInsertion(cache2, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   @Test
   public void testLocalNodeOwnerAndClusterListener() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      MagicKey key = new MagicKey(cache0);

      verifySimpleInsertion(cache0, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   @Test
   public void testLocalNodeNonOwnerAndClusterListener() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = listener();
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
      testFilter(keyToFilterOut, new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)), 1000l,
              new KeyValueFilterAsCacheEventFilter(new LifespanFilter<Object, String>(100)));
   }

   @Test
   public void testMetadataFilterLocalOnly() {
      final String keyToFilterOut = "filter-me";
      testFilter(keyToFilterOut, new MagicKey(cache(0, CACHE_NAME)), 1000l,
              new KeyValueFilterAsCacheEventFilter(new LifespanFilter<Object, String>(100)));
   }

   protected void testSimpleFilter(Object key) {
      final String keyToFilterOut = "filter-me";
      testFilter(keyToFilterOut, key, null, new KeyFilterAsCacheEventFilter<Object>(
              new CollectionKeyFilter(Collections.singleton(key), true)));
   }

   protected void testFilter(Object keyToFilterOut, Object keyToUse, Long lifespan, CacheEventFilter<? super Object, ? super String> filter) {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterListener clusterListener = listener();
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

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener, null, new StringTruncator(0, 2));

      verifySimpleInsertion(cache0, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE.substring(0, 2));
   }

   protected <C> void testConverter(Object key, String value, Object resultingValue, Long lifespan,
                                    CacheEventConverter<Object, ? super String, C> converter) {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener, null, converter);

      verifySimpleInsertion(cache0, key, value, lifespan, clusterListener, resultingValue);
   }

   @Test
   public void testClusterListenerNodeGoesDown() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      int cache1ListenerSize = cache1.getAdvancedCache().getListeners().size();
      int cache2ListenerSize = cache2.getAdvancedCache().getListeners().size();

      log.info("Killing node 0 ..");
      TestingUtil.killCacheManagers(manager(0));
      cacheManagers.remove(0);
      log.info("Node 0 killed");

      TestingUtil.blockUntilViewsReceived(60000, false, cache1, cache2);
      TestingUtil.waitForNoRebalance(cache1, cache2);

      assertEquals(cache1.getAdvancedCache().getListeners().size(), cache1ListenerSize -
            (cacheMode.isDistributed() ? 1 : 0));
      assertEquals(cache2.getAdvancedCache().getListeners().size(), cache2ListenerSize -
              (cacheMode.isDistributed() ? 1 : 0));
   }

   @Test
   public void testNodeComesUpWithClusterListenerAlreadyInstalled() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterListener clusterListener = listener();
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

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener, new KeyFilterAsCacheEventFilter<Object>(
              new CollectionKeyFilter<Object>(Collections.singleton(keyToFilter), true)), new StringTruncator(0, 3));

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

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      // Adding a cluster listener should add to each node in cluster
      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 1);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize +
              (cacheMode.isDistributed() ? 1 : 0));
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize +
              (cacheMode.isDistributed() ? 1 : 0));

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

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      // Adding a cluster listener should add to each node in cluster
      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 1);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize +
            (cacheMode.isDistributed() ? 1 : 0));
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize +
            (cacheMode.isDistributed() ? 1 : 0));

      ClusterListener clusterListener2 = listener();
      cache0.addListener(clusterListener2);

      // Adding a second cluster listener should add to each node in cluster as well
      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 2);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize +
            (cacheMode.isDistributed() ? 2 : 0));
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize +
            (cacheMode.isDistributed() ? 2 : 0));

      MagicKey key = new MagicKey(cache2, cache1);
      cache1.put(key, FIRST_VALUE);

      // Both listeners should have been notified
      assertEquals(clusterListener.events.size(), 1);
      assertEquals(clusterListener2.events.size(), 1);

      verifySimpleInsertionEvents(clusterListener, key, FIRST_VALUE);
      verifySimpleInsertionEvents(clusterListener2, key, FIRST_VALUE);

      cache0.removeListener(clusterListener);

      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 1);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize +
            (cacheMode.isDistributed() ? 1 : 0));
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize +
            (cacheMode.isDistributed() ? 1 : 0));

      // Change the value again to make sure other listener is still working properly
      cache2.put(key, SECOND_VALUE);

      assertEquals(clusterListener2.events.size(), 2);

      CacheEntryEvent event = clusterListener2.events.get(1);

      assertEquals(Event.Type.CACHE_ENTRY_MODIFIED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(SECOND_VALUE, ((CacheEntryModifiedEvent)event).getValue());
   }

   @Test
   public void testMemberLeavesThatClusterListenerNotNotified() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      Object key = new MagicKey(cache1, cache2);
      cache1.put(key, "some-key");

      final ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      log.info("Killing node 1 ..");
      TestingUtil.killCacheManagers(manager(1));
      cacheManagers.remove(1);
      log.info("Node 1 killed");

      TestingUtil.blockUntilViewsReceived(10000, false, cacheManagers);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      assertEquals(clusterListener.hasIncludeState() ? 1 : 0, clusterListener.events.size());
   }

   @Test
   public void testPreviousValueConverterEventRaisedLocalNode() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      String previousValue = "myOldValue";
      long previousExpiration = 10000000;
      MagicKey key = new MagicKey(cache0);

      cache0.put(key, previousValue, previousExpiration, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener, null, new StringAppender());

      String newValue = "myBrandSpankingNewValue";
      long newExpiration = 314159;
      verifySimpleModification(cache0, key, newValue, newExpiration, clusterListener,
              previousValue + previousExpiration + newValue + newExpiration);

   }

   @Test
   public void testPreviousValueConverterEventRaisedNonOwnerNode() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      String previousValue = "myOldValue";
      long previousExpiration = 10000000;
      MagicKey key = new MagicKey(cache0, cache1);

      cache0.put(key, previousValue, previousExpiration, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache2.addListener(clusterListener, null, new StringAppender());

      String newValue = "myBrandSpankingNewValue";
      long newExpiration = 314159;
      verifySimpleModification(cache0, key, newValue, newExpiration, clusterListener,
              previousValue + previousExpiration + newValue + newExpiration);
   }

   @Test
   public void testPreviousValueConverterEventRaisedBackupOwnerNode() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      String previousValue = "myOldValue";
      long previousExpiration = 10000000;
      MagicKey key = new MagicKey(cache0, cache1);

      cache0.put(key, previousValue, previousExpiration, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache1.addListener(clusterListener, null, new StringAppender());

      String newValue = "myBrandSpankingNewValue";
      long newExpiration = 314159265;
      verifySimpleModification(cache0, key, newValue, newExpiration, clusterListener,
                               previousValue + previousExpiration + newValue + newExpiration);
   }

   @Test
   public void testPreviousValueFilterEventRaisedLocalNode() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      String previousValue = "myOldValue";
      long previousExpiration = 10000000;
      MagicKey key = new MagicKey(cache0, cache1);

      cache0.put(key, previousValue, previousExpiration, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache1.addListener(clusterListener, new NewLifespanLargerFilter<Object, String>(), null);

      // This event is ignored because lifespan is shorter
      cache0.put(key, previousValue, previousExpiration - 100, TimeUnit.MILLISECONDS);

      String newValue = "myBrandSpankingNewValue";
      long newExpiration = 314159265;
      verifySimpleModification(cache0, key, newValue, newExpiration, clusterListener, newValue);
   }

   @Test
   public void testPreviousValueFilterEventRaisedNonOwnerNode() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      String previousValue = "myOldValue";
      long previousExpiration = 10000000;
      MagicKey key = new MagicKey(cache0, cache1);
      // This event is ignored because no previous lifespan
      cache0.put(key, previousValue, previousExpiration, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache2.addListener(clusterListener, new NewLifespanLargerFilter<Object, String>(), null);

      // This event is ignored because lifespan is shorter
      cache0.put(key, previousValue, previousExpiration - 100, TimeUnit.MILLISECONDS);

      String newValue = "myBrandSpankingNewValue";
      long newExpiration = 314159265;
      verifySimpleModification(cache0, key, newValue, newExpiration, clusterListener, newValue);
   }

   @Test
   public void testPreviousValueFilterEventRaisedBackupOwnerNode() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      String previousValue = "myOldValue";
      long previousExpiration = 10000000;
      MagicKey key = new MagicKey(cache0, cache1);
      // This event is ignored because no previous lifespan
      cache0.put(key, previousValue, previousExpiration, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache1.addListener(clusterListener, new NewLifespanLargerFilter<Object, String>(), null);

      // This event is ignored because lifespan is shorter
      cache0.put(key, previousValue, previousExpiration - 100, TimeUnit.MILLISECONDS);

      String newValue = "myBrandSpankingNewValue";
      long newExpiration = 314159265;
      verifySimpleModification(cache0, key, newValue, newExpiration, clusterListener, newValue);
   }

   @Test
   public void testCacheEventFilterConverter() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(0, CACHE_NAME);
      Cache<Object, String> cache2 = cache(0, CACHE_NAME);

      String convertedValue = "my-value";
      FilterConverter filterConverter = new FilterConverter(true, convertedValue);
      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener, filterConverter, filterConverter);

      MagicKey key = new MagicKey(cache1, cache2);

      verifySimpleInsertion(cache0, key, "doesn't-matter", null, clusterListener, convertedValue);
   }

   @Test
   public void testListenerOnPrimaryNodeReadPrimary() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      MagicKey key = new MagicKey(cache0);

      String expectedValue = key + "-expiring";
      cache0.put(key, key + "-expiring", 1000, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      ts0.advance(1001);

      assertNull(cache0.get(key));

      int expectCount = clusterListener.hasIncludeState() ? 2 : 1;
      eventually(() -> clusterListener.events.size() >= expectCount, 200000);
      assertEquals(expectCount, clusterListener.events.size());
      CacheEntryEvent event = clusterListener.events.get(clusterListener.hasIncludeState() ? 1 : 0);

      assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(expectedValue, event.getValue());
   }

   @Test
   public void testListenerOnPrimaryNodeReadBackup() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      MagicKey key = new MagicKey(cache0, cache1);

      String expectedValue = key + "-expiring";
      cache0.put(key, key + "-expiring", 1000, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      ts1.advance(1001);

      assertNull(cache1.get(key));

      int expectCount = clusterListener.hasIncludeState() ? 2 : 1;
      eventually(() -> clusterListener.events.size() >= expectCount);
      assertEquals(expectCount, clusterListener.events.size());
      CacheEntryEvent event = clusterListener.events.get(clusterListener.hasIncludeState() ? 1 : 0);

      assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(expectedValue, event.getValue());
   }

   public void testListenerOnBackupOwnerNodePrimaryRead() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      MagicKey key = new MagicKey(cache0, cache1);

      String expectedValue = key + "-expiring";
      cache0.put(key, key + "-expiring", 1000, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache1.addListener(clusterListener);

      ts0.advance(1001);

      assertNull(cache0.get(key));

      int expectCount = clusterListener.hasIncludeState() ? 2 : 1;
      eventually(() -> clusterListener.events.size() >= expectCount);
      assertEquals(expectCount, clusterListener.events.size());
      CacheEntryEvent event = clusterListener.events.get(clusterListener.hasIncludeState() ? 1 : 0);

      assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(expectedValue, event.getValue());
   }

   public void testListenerOnBackupOwnerNodeBackupRead() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      MagicKey key = new MagicKey(cache0, cache1);

      String expectedValue = key + "-expiring";
      cache0.put(key, key + "-expiring", 1000, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      ts1.advance(1001);

      assertNull(cache1.get(key));

      int expectCount = clusterListener.hasIncludeState() ? 2 : 1;
      eventually(() -> clusterListener.events.size() >= expectCount);
      assertEquals(expectCount, clusterListener.events.size());
      CacheEntryEvent event = clusterListener.events.get(clusterListener.hasIncludeState() ? 1 : 0);

      assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(expectedValue, event.getValue());
   }

   public void testAllExpire() throws InterruptedException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      MagicKey key = new MagicKey(cache0);

      String expectedValue = key + "-expiring";
      cache0.put(key, key + "-expiring", 1000, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      ts0.advance(1001);
      ts1.advance(1001);
      ts2.advance(1001);

      assertNull(cache0.get(key));
      assertNull(cache1.get(key));
      assertNull(cache2.get(key));

      int expectCount = clusterListener.hasIncludeState() ? 2 : 1;
      eventually(() -> "Expected: " + expectCount + " Actual: " + clusterListener.events,
            () -> clusterListener.events.size() >= expectCount);
      // We can't assert size here, thiis is because expiration is done asynchronously.  As such you could have more
      // than 1 expire command come at the same time, although from different nodes.  Currently we assume a null is
      // okay to say it was expired, so you can get multiple expirations
      CacheEntryEvent event = clusterListener.events.get(clusterListener.hasIncludeState() ? 1 : 0);

      assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(expectedValue, event.getValue());
   }

   @Test
   public void testSimpleExpirationFilterNotOwner() {
      testSimpleExpirationFilter(new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)));
   }

   @Test
   public void testExpirationMetadataFilterNotOwner() {
      final String keyToFilterOut = "filter-me";
      testExpirationFilter(keyToFilterOut, 50l, new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)), 1000l,
              new KeyValueFilterAsCacheEventFilter(new LifespanFilter<Object, String>(100)));
   }

   protected void testSimpleExpirationFilter(Object key) {
      final String keyToFilterOut = "filter-me";
      final long commonLifespan = 1000l;
      testExpirationFilter(keyToFilterOut, commonLifespan, key, commonLifespan, new KeyFilterAsCacheEventFilter<Object>(
              new CollectionKeyFilter(Collections.singleton(key), true)));
   }

   protected void testExpirationFilter(Object keyToFilterOut, Long keyToFilterOutLifespan, Object keyToUse, Long keyToUselifespan, CacheEventFilter<? super Object, ? super String> filter) {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      //put from a non-owner
      cache0.put(keyToFilterOut, FIRST_VALUE, keyToFilterOutLifespan, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener, filter, null);

      ts0.advance(keyToFilterOutLifespan + 1);
      ts1.advance(keyToFilterOutLifespan + 1);
      ts2.advance(keyToFilterOutLifespan + 1);

      assertNull(cache0.get(keyToFilterOut));
       // We should not have gotten the message since it was filtered
      assertEquals(clusterListener.events.size(), 0);

      String expectedValue = keyToUse + "-expiring";
      cache0.put(keyToUse, keyToUse + "-expiring", keyToUselifespan, TimeUnit.MILLISECONDS);

      ts0.advance(keyToUselifespan + 1);
      ts1.advance(keyToUselifespan + 1);
      ts2.advance(keyToUselifespan + 1);

      assertNull(cache0.get(keyToUse));

      verifySimpleExpirationEvents(clusterListener, 2, keyToUse, expectedValue);
   }

   @Test
   public void testSimpleExpirationConverterNotOwner() {
      long lifespan = 1000l;
      StringTruncator converter = new StringTruncator(0, 2);
      testExpirationConverter(new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)), FIRST_VALUE, FIRST_VALUE.substring(0, 2), lifespan, converter);
   }

   @Test
   public void testMetadataExpirationConverterSuccessNotOwner() {
      long lifespan = 25000l;
      LifespanConverter converter = new LifespanConverter(true, 500);
      testExpirationConverter(new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME)), FIRST_VALUE, lifespan, lifespan, converter);
   }

   protected <C> void testExpirationConverter(Object key, String value, Object expectedValue, Long lifespan,
                                    CacheEventConverter<Object, ? super String, C> converter) {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      cache0.put(key, value, lifespan, TimeUnit.MILLISECONDS);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener, null, converter);

      ts0.advance(lifespan + 1);
      ts1.advance(lifespan + 1);
      ts2.advance(lifespan + 1);

      assertNull(cache0.get(key));

      verifySimpleExpirationEvents(clusterListener, clusterListener.hasIncludeState() ? 2 : 1, key, expectedValue);
   }

}
