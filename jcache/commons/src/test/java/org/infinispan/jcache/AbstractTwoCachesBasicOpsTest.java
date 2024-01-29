package org.infinispan.jcache;

import static org.infinispan.jcache.util.JCacheTestingUtil.getEntryCount;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Base class for clustered JCache tests. Implementations must provide cache references.
 *
 * @author Matej Cimbora
 */
@Test(testName = "org.infinispan.jcache.AbstractTwoCachesBasicOpsTest", groups = {"functional", "smoke"})
public abstract class AbstractTwoCachesBasicOpsTest extends MultipleCacheManagersTest {

   @Test
   public void testCacheManagerXmlConfig(Method m) {
      Cache<String, String> cache = getCache1(m);

      cache.put(k(m), v(m));
      assertEquals(cache.get(k(m)), v(m));
   }

   @Test
   public void testMultipleCacheManagersXmlConfig(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      assertEquals(cache1.get(k(m)), v(m));
      assertEquals(cache2.get(k(m)), v(m));
   }

   @Test
   public void testRemove(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      assertTrue(cache1.containsKey(k(m)));
      assertTrue(cache2.containsKey(k(m)));
      cache2.remove(k(m));
      assertFalse(cache1.containsKey(k(m)));
      assertFalse(cache2.containsKey(k(m)));
   }

   @Test
   public void testRemoveAll(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      cache1.put(k(m, 2), v(m, 2));
      assertTrue(cache1.containsKey(k(m)));
      assertTrue(cache1.containsKey(k(m, 2)));
      assertTrue(cache2.containsKey(k(m)));
      assertTrue(cache2.containsKey(k(m, 2)));

      Set<String> keySet = new HashSet<>();
      keySet.add(k(m));
      cache2.removeAll(keySet);
      assertFalse(cache1.containsKey(k(m)));
      assertTrue(cache1.containsKey(k(m, 2)));
      assertFalse(cache2.containsKey(k(m)));
      assertTrue(cache2.containsKey(k(m, 2)));

      cache1.removeAll();
      assertFalse(cache1.containsKey(k(m, 2)));
      assertFalse(cache2.containsKey(k(m, 2)));
   }

   @Test
   public void testPutAll(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      Map<String, String> entryMap = new HashMap<>();
      entryMap.put(k(m), v(m));
      entryMap.put(k(m, 2), v(m, 2));
      cache1.putAll(entryMap);
      assertTrue(cache1.containsKey(k(m)));
      assertTrue(cache1.containsKey(k(m, 2)));
      assertTrue(cache2.containsKey(k(m)));
      assertTrue(cache2.containsKey(k(m, 2)));
   }

   @Test
   public void testPutAllMapNullValuesNotAllowed(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Map<String, String> entryMap = new ConcurrentHashMap<>();
      entryMap.put(k(m), v(m));
      entryMap.put(k(m, 2), v(m, 2));
      cache1.putAll(entryMap);
      assertTrue(cache1.containsKey(k(m)));
      assertTrue(cache1.containsKey(k(m, 2)));
   }

   @Test (expectedExceptions = NullPointerException.class)
   public void testPutAllNullMap(Method m) {
      getCache1(m).putAll(null);
   }

   @Test (expectedExceptions = NullPointerException.class)
   public void testPutAllMapWithNullKeys(Method m) {
      Map<String, String> entryMap = new HashMap<>();
      entryMap.put(null, v(m));
      getCache1(m).putAll(entryMap);
   }

   @Test (expectedExceptions = NullPointerException.class)
   public void testPutAllMapWithNullValues(Method m) {
      Map<String, String> entryMap = new HashMap<>();
      entryMap.put(k(m), null);
      getCache1(m).putAll(entryMap);
   }

   @Test
   public void testPutIfAbsent(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      cache2.putIfAbsent(k(m), v(m, 3));
      assertEquals(cache1.get(k(m)), v(m));
      assertEquals(cache2.get(k(m)), v(m));

      cache1.putIfAbsent(k(m, 2), v(m, 2));
      assertTrue(cache1.containsKey(k(m, 2)));
      assertTrue(cache2.containsKey(k(m, 2)));
   }

   @Test
   public void testGetAndPut(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      String result = cache2.getAndPut(k(m), v(m, 2));
      assertEquals(result, v(m));
      assertEquals(cache1.get(k(m)), v(m, 2));
      assertEquals(cache2.get(k(m)), v(m, 2));
   }

   @Test
   public void testGetAndRemove(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      String result = cache2.getAndRemove(k(m));
      assertEquals(result, v(m));
      assertFalse(cache1.containsKey(k(m)));
      assertFalse(cache2.containsKey(k(m)));
   }

   @Test
   public void testGetAndReplace(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      String result = cache2.getAndReplace(k(m), v(m, 2));
      assertEquals(result, v(m));
      assertEquals(cache1.get(k(m)), v(m, 2));
      assertEquals(cache2.get(k(m)), v(m, 2));
   }

   @Test
   public void testGetAll(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      cache1.put(k(m, 2), v(m, 2));
      Set<String> keySet = new HashSet<>();
      keySet.add(k(m));
      keySet.add(k(m, 2));
      Map<String, String> result = cache2.getAll(keySet);
      assertEquals(result.size(), 2);
      assertTrue(result.containsKey(k(m)));
      assertTrue(result.containsKey(k(m, 2)));
      result = cache1.getAll(keySet);
      assertEquals(result.size(), 2);
      assertTrue(result.containsKey(k(m)));
      assertTrue(result.containsKey(k(m, 2)));
   }

   @Test
   public void testReplace(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      assertTrue(cache1.containsKey(k(m)));
      assertTrue(cache2.containsKey(k(m)));

      cache2.replace(k(m), v(m, 2));
      assertEquals(cache1.get(k(m)), v(m, 2));
      assertEquals(cache2.get(k(m)), v(m, 2));

      cache1.replace(k(m), v(m, 2), v(m, 3));
      assertEquals(cache1.get(k(m)), v(m, 3));
      assertEquals(cache2.get(k(m)), v(m, 3));

      boolean result = cache2.replace(k(m), "staleValue", v(m, 4));
      assertFalse(result);
      assertEquals(cache1.get(k(m)), v(m, 3));
      assertEquals(cache2.get(k(m)), v(m, 3));
   }

   @Test
   public void testIterator(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      for (int i = 0; i < 5; i++) {
         cache1.put(k(m, i), v(m, i));
      }
      assertEquals(getEntryCount(cache1.iterator()), 5);
      assertEquals(getEntryCount(cache2.iterator()), 5);
   }

   @Test
   public void testClusteredClear(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      cache1.put(k(m, 2), v(m, 2));
      cache2.clear();
      assertNull(cache1.get(k(m)));
      assertNull(cache2.get(k(m)));
      assertNull(cache1.get(k(m, 2)));
      assertNull(cache2.get(k(m, 2)));
   }

   @Test
   public void testEntryProcessor(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      cache2.invoke(k(m), new CustomEntryProcessor());
      assertEquals(cache1.get(k(m)), v(m) + "_processed");
      assertEquals(cache2.get(k(m)), v(m) + "_processed");
   }

   @Test
   public void testUpdatedListener(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      TestUpdatedListener listener = new TestUpdatedListener();
      MutableCacheEntryListenerConfiguration<String, String> conf = new MutableCacheEntryListenerConfiguration<>(FactoryBuilder.factoryOf(listener), null, false, true);
      try {
         cache1.registerCacheEntryListener(conf);
         cache1.put(k(m), v(m, 2));
         cache2.put(k(m), v(m, 3));

         eventuallyEquals(1, listener::getInvocationCount);
         CacheEntryEvent<?, ?> event = listener.getEvent(0);
         assertEquals(k(m), event.getKey());
         assertEquals(v(m, 3), event.getValue());
         assertNull(event.getOldValue());

         cache1.deregisterCacheEntryListener(conf);
         listener.reset();

         cache2.put(k(m), v(m, 4));

         TestingUtil.sleepThread(50);
         assertEquals(0, listener.getInvocationCount());
      } finally {
         cache1.deregisterCacheEntryListener(conf);
      }
   }

   @Test
   public void testRemovedListener(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      TestRemovedListener listener = new TestRemovedListener();
      MutableCacheEntryListenerConfiguration<String, String> conf = new MutableCacheEntryListenerConfiguration<>(FactoryBuilder.factoryOf(listener), null, false, true);
      try {
         cache1.registerCacheEntryListener(conf);
         cache1.put(k(m), v(m, 3));
         cache2.remove(k(m));

         eventuallyEquals(1, listener::getInvocationCount);
         CacheEntryEvent<?, ?> event = listener.getEvent(0);
         assertEquals(k(m), event.getKey());
         assertNull(event.getOldValue());
         assertNull(event.getValue());

         cache1.deregisterCacheEntryListener(conf);
         listener.reset();

         cache1.put(k(m, 2), v(m, 2));
         cache2.remove(k(m, 2));

         TestingUtil.sleepThread(50);
         assertEquals(0, listener.getInvocationCount());
      } finally {
         cache1.deregisterCacheEntryListener(conf);
      }
   }

   @Test
   public void testCreatedListener(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      TestCreatedListener listener = new TestCreatedListener();
      MutableCacheEntryListenerConfiguration<String, String> conf = new MutableCacheEntryListenerConfiguration<>(FactoryBuilder.factoryOf(listener), null, false, true);
      try {
         cache1.registerCacheEntryListener(conf);
         cache2.put(k(m), v(m, 3));

         eventuallyEquals(1, listener::getInvocationCount);
         CacheEntryEvent<?, ?> event = listener.getEvent(0);
         assertEquals(k(m), event.getKey());
         assertEquals(v(m, 3), event.getValue());
         assertNull(event.getOldValue());

         cache1.deregisterCacheEntryListener(conf);
         listener.reset();

         cache2.put(k(m, 2), v(m, 2));
         TestingUtil.sleepThread(50);
         assertEquals(0, listener.getInvocationCount());
      } finally {
         cache1.deregisterCacheEntryListener(conf);
      }
   }

   @Test
   public void testListenerDiscardingFilter(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      DiscardingCacheEntryEventFilter filter = new DiscardingCacheEntryEventFilter();
      TestCreatedListener listener1 = new TestCreatedListener();
      TestCreatedListener listener2 = new TestCreatedListener();
      MutableCacheEntryListenerConfiguration<String, String> conf1 = new MutableCacheEntryListenerConfiguration<>(FactoryBuilder.factoryOf(listener1), null, false, true);
      MutableCacheEntryListenerConfiguration<String, String> conf2 = new MutableCacheEntryListenerConfiguration<>(FactoryBuilder.factoryOf(listener2), FactoryBuilder.factoryOf(filter), false, true);
      try {
         cache1.registerCacheEntryListener(conf1);
         cache2.registerCacheEntryListener(conf2);
         cache2.put(k(m), v(m, 3));
         eventuallyEquals(1, listener1::getInvocationCount);
         assertEquals(0, listener2.getInvocationCount());

         cache1.deregisterCacheEntryListener(conf1);
         listener1.reset();

         cache2.put(k(m, 2), v(m, 2));
         TestingUtil.sleepThread(50);
         assertEquals(0, listener1.getInvocationCount());
      } finally {
         cache1.deregisterCacheEntryListener(conf1);
         cache2.deregisterCacheEntryListener(conf2);
      }
   }

   private static class DiscardingCacheEntryEventFilter implements CacheEntryEventFilter<Object, Object>, Serializable {

      @Override
      public boolean evaluate(CacheEntryEvent<?, ?> event) throws CacheEntryListenerException {
         return false;
      }
   }

   private static class TestUpdatedListener extends InvocationAwareListener implements CacheEntryUpdatedListener<Object, Object>, Serializable {

      @Override
      public void onUpdated(Iterable<CacheEntryEvent<?, ?>> cacheEntryEvents) throws CacheEntryListenerException {
         cacheEntryEvents.forEach(this::addEvent);
      }
   }

   private static class TestCreatedListener extends InvocationAwareListener implements CacheEntryCreatedListener<Object, Object>, Serializable {

      @Override
      public void onCreated(Iterable<CacheEntryEvent<?, ?>> cacheEntryEvents) throws CacheEntryListenerException {
         cacheEntryEvents.forEach(this::addEvent);
      }
   }

   private static class TestRemovedListener extends InvocationAwareListener implements CacheEntryRemovedListener<Object, Object>, Serializable {

      @Override
      public void onRemoved(Iterable<CacheEntryEvent<?, ?>> cacheEntryEvents) throws CacheEntryListenerException {
         cacheEntryEvents.forEach(this::addEvent);
      }
   }

   private abstract static class InvocationAwareListener {

      protected List<CacheEntryEvent<?, ?>> events = Collections.synchronizedList(new ArrayList<>());

      public synchronized int getInvocationCount() {
         return events.size();
      }

      public synchronized CacheEntryEvent<?, ?> getEvent(int i) {
         return events.get(i);
      }

      public synchronized void reset() {
         events.clear();
      }

      public synchronized void addEvent(CacheEntryEvent<?, ?> e) {
         this.events.add(e);
      }

      @Override
      public String toString() {
         return getClass().getSimpleName() + "{" +
                "events=" + events +
                '}';
      }
   }

   @ProtoName("CustomEntryProcessor")
   public static class CustomEntryProcessor implements EntryProcessor<String, String, Object> {
      @Override
      public Object process(MutableEntry<String, String> entry, Object... arguments) throws EntryProcessorException {
         entry.setValue(entry.getValue() + "_processed");
         return entry;
      }
   }

   public abstract Cache<String, String> getCache1(Method m);
   public abstract Cache<String, String> getCache2(Method m);
}
