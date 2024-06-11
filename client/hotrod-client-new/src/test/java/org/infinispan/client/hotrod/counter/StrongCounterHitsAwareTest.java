package org.infinispan.client.hotrod.counter;

import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.HitsAwareCacheManagersTest;
import org.infinispan.client.hotrod.RemoteCounterManagerFactory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.util.ByteString;
import org.testng.annotations.Test;

/**
 * Tests if the {@link org.infinispan.counter.api.StrongCounter} operations hits the primary owner of the counter to
 * save some network hops.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
@Test(groups = "functional", testName = "client.hotrod.counter.StrongCounterHitsAwareTest")
public class StrongCounterHitsAwareTest extends HitsAwareCacheManagersTest {

   private static final int NUM_SERVERS = 3;

   public void testAddAndGetHits(Method method) {
      String counterName = method.getName();
      doTest(counterName, SyncStrongCounter::incrementAndGet);
   }

   public void testGetValueHits(Method method) {
      String counterName = method.getName();
      doTest(counterName, SyncStrongCounter::getValue);
   }

   public void testResetHits(Method method) {
      String counterName = method.getName();
      doTest(counterName, SyncStrongCounter::reset);
   }

   public void testCompareAndSwapHits(Method method) {
      String counterName = method.getName();
      doTest(counterName, counter -> counter.compareAndSwap(0, 1));
   }

   public void testRemoveHits(Method method) {
      String counterName = method.getName();
      doTest(counterName, SyncStrongCounter::remove);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, new ConfigurationBuilder());
      waitForClusterToForm(CounterModuleLifecycle.COUNTER_CACHE_NAME);
      addInterceptors(CounterModuleLifecycle.COUNTER_CACHE_NAME);
      assertEquals(NUM_SERVERS, getCacheManagers().size());
   }

   @Override
   protected void resetStats() {
      caches(CounterModuleLifecycle.COUNTER_CACHE_NAME).stream().map(this::getHitCountInterceptor)
            .forEach(HitCountInterceptor::reset);
   }

   private void doTest(String counterName, Consumer<SyncStrongCounter> action) {
      defineCounter(counterName);

      int primaryOwner = findPrimaryOwnerIndex(counterName);

      //lets hope there are no commands still going throw the cluster :)
      resetStats();

      for (CounterManager cm : clientCounterManagers()) {
         action.accept(cm.getStrongCounter(counterName).sync());
      }

      assertHits(primaryOwner);
   }

   private void defineCounter(String counterName) {
      CounterManager counterManager = RemoteCounterManagerFactory.asCounterManager(client(0));
      counterManager.defineCounter(counterName, CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
      //the counter is lazily created and stored in the cache. The increment will make sure it is there!
      for (CounterManager cm : clientCounterManagers()) {
         cm.getStrongCounter(counterName).sync().incrementAndGet();
      }
   }

   private void assertHits(int primaryOwnerIndex) {
      for (int i = 0; i < NUM_SERVERS; ++i) {
         Cache<?, ?> cache = cache(i, CounterModuleLifecycle.COUNTER_CACHE_NAME);
         HitCountInterceptor interceptor = getHitCountInterceptor(cache);
         if (i == primaryOwnerIndex) {
            assertEquals("Wrong number of hits on primary owner", NUM_SERVERS, interceptor.getHits());
         } else {
            assertEquals(
                  "Wrong number of hits on " + address(i) + ". Primary owner is " + address(primaryOwnerIndex), 0,
                  interceptor.getHits());
         }
      }
   }

   private int findPrimaryOwnerIndex(String counterName) {
      CounterKey key = findCounterKey(counterName);
      for (int i = 0; i < NUM_SERVERS; ++i) {
         Cache<CounterKey, CounterValue> cache = cache(i, CounterModuleLifecycle.COUNTER_CACHE_NAME);
         if (cache.getAdvancedCache().getDistributionManager().getCacheTopology().getDistribution(key).isPrimary()) {
            return i;
         }
      }
      throw new IllegalStateException();
   }

   private CounterKey findCounterKey(String counterName) {
      ByteString bs = ByteString.fromString(counterName);
      Cache<CounterKey, CounterValue> cache = cache(0, CounterModuleLifecycle.COUNTER_CACHE_NAME);
      List<CounterKey> keys = new ArrayList<>(cache.keySet());
      for (CounterKey counterKey : keys) {
         if (counterKey.getCounterName().equals(bs)) {
            return counterKey;
         }
      }
      throw new IllegalStateException();
   }

   private List<CounterManager> clientCounterManagers() {
      return clients.stream().map(RemoteCounterManagerFactory::asCounterManager).collect(Collectors.toList());
   }
}
