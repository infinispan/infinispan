package org.infinispan.notifications.cachelistener.cluster;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test to ensure when a rehash occurs that cluster listeners are not notified.
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "org.infinispan.notifications.cachelistener.cluster.RehashClusterListenerTest")
public class RehashClusterListenerTest extends MultipleCacheManagersTest {
   protected final static String CACHE_NAME = "cluster-listener";
   protected final static String KEY = "key";
   protected final static String VALUE = "value";

   protected ConfigurationBuilder builderUsed;

   protected final ControlledConsistentHashFactory factory;

   public RehashClusterListenerTest() {
      this.factory = null;
   }

   public RehashClusterListenerTest(ControlledConsistentHashFactory factory) {
      this.factory = factory;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
         new RehashClusterListenerTest(new ControlledConsistentHashFactory.Default(1, 2)).cacheMode(CacheMode.DIST_SYNC),
      };
   }

   @BeforeMethod
   protected void beforeMethod() throws Exception {
      factory.setOwnerIndexes(1, 2);
      factory.triggerRebalance(cache(0, CACHE_NAME));
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode).hash().consistentHashFactory(factory).numSegments(1);
      createClusteredCaches(3, ControlledConsistentHashFactory.SCI.INSTANCE, CACHE_NAME, builderUsed);
   }

   public void testClusterListenerNodeBecomingPrimaryFromNotAnOwner() throws Exception {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      cache1.put(KEY, VALUE);

      ClusterListener listener = new ClusterListener();
      cache0.addListener(listener);

      factory.setOwnerIndexes(0, 1);

      log.trace("Triggering rebalance to cause segment ownership to change");
      factory.triggerRebalance(cache0);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache0.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).containsKey(KEY);
         }
      });

      TestingUtil.waitForNoRebalance(cache0, cache1, cache2);

      assertEquals(listener.events.size(), 0);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testClusterListenerNodeBecomingBackupFromNotAnOwner() throws Exception {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      cache1.put(KEY, VALUE);

      ClusterListener listener = new ClusterListener();
      cache0.addListener(listener);

      factory.setOwnerIndexes(1, 0);

      log.trace("Triggering rebalance to cause segment ownership to change");
      factory.triggerRebalance(cache0);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache0.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).containsKey(KEY);
         }
      });

      TestingUtil.waitForNoRebalance(cache0, cache1, cache2);

      assertEquals(listener.events.size(), 0);
   }

   public void testOtherNodeBecomingBackupFromNotAnOwner() throws Exception {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      cache1.put(KEY, VALUE);

      ClusterListener listener = new ClusterListener();
      cache2.addListener(listener);

      factory.setOwnerIndexes(1, 0);

      log.trace("Triggering rebalance to cause segment ownership to change");
      factory.triggerRebalance(cache0);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            if (cacheMode.isDistributed()) {
               return cache0.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_OWNERSHIP_CHECK).containsKey(KEY);
            }
            // Scattered always writes to next owner and 1 is always the primary
            return cache2.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_OWNERSHIP_CHECK).containsKey(KEY);
         }
      });

      TestingUtil.waitForNoRebalance(cache0, cache1, cache2);

      assertEquals(listener.events.size(), 0);
   }

   public void testOtherNodeBecomingPrimaryFromNotAnOwner() throws Exception {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      cache1.put(KEY, VALUE);

      ClusterListener listener = new ClusterListener();
      cache2.addListener(listener);

      factory.setOwnerIndexes(0, 1);

      log.trace("Triggering rebalance to cause segment ownership to change");
      factory.triggerRebalance(cache0);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache0.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).containsKey(KEY);
         }
      });

      TestingUtil.waitForNoRebalance(cache0, cache1, cache2);

      assertEquals(listener.events.size(), 0);
   }

   @Listener(clustered = true)
   protected static class ClusterListener {
      List<CacheEntryEvent> events = Collections.synchronizedList(new ArrayList<CacheEntryEvent>());

      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      public void onCacheEvent(CacheEntryEvent event) {
         log.debugf("Adding new cluster event %s", event);
         events.add(event);
      }
   }
}
