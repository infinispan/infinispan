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
import java.util.Iterator;
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

import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.eventually.Eventually;
import org.testng.annotations.Test;

/**
 * Base class for clustered JCache tests. Implementations must provide cache references.
 *
 * @author Matej Cimbora
 */
@Test(testName = "org.infinispan.jcache.AbstractTwoCachesBasicOpsTest", groups = "functional")
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
      cache2.invoke(k(m), new CustomEntryProcessor(), null);
      assertEquals(cache1.get(k(m)), v(m) + "_processed");
      assertEquals(cache2.get(k(m)), v(m) + "_processed");
   }

   @Test
   public void testUpdatedListener(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      TestUpdatedListener listener = new TestUpdatedListener();
      MutableCacheEntryListenerConfiguration conf = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener), null, false, true);
      try {
         cache1.registerCacheEntryListener(conf);
         cache1.put(k(m), v(m, 2));
         cache2.put(k(m), v(m, 3));

         Eventually.eventually(() -> listener.toString(), () -> listener.getInvocationCount() == 1);

         cache1.deregisterCacheEntryListener(conf);
         listener.reset();

         cache2.put(k(m), v(m, 4));

         Eventually.eventually(() -> listener.toString(), () -> listener.getInvocationCount() == 0);
      } finally {
         cache1.deregisterCacheEntryListener(conf);
      }
   }

   @Test
   public void testRemovedListener(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      TestRemovedListener listener = new TestRemovedListener();
      MutableCacheEntryListenerConfiguration conf = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener), null, false, true);
      try {
         cache1.registerCacheEntryListener(conf);
         cache1.put(k(m), v(m, 3));
         cache2.remove(k(m));
         Eventually.eventually(() -> listener.toString(), () -> listener.getInvocationCount() == 1);

         cache1.deregisterCacheEntryListener(conf);
         listener.reset();

         cache1.put(k(m, 2), v(m, 2));
         cache2.remove(k(m, 2));

         Eventually.eventually(() -> listener.toString(), () -> listener.getInvocationCount() == 0);
      } finally {
         cache1.deregisterCacheEntryListener(conf);
      }
   }

   @Test
   public void testCreatedListener(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      TestCreatedListener listener = new TestCreatedListener();
      MutableCacheEntryListenerConfiguration conf = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener), null, false, true);
      try {
         cache1.registerCacheEntryListener(conf);
         cache2.put(k(m), v(m, 3));
         Eventually.eventually(() -> listener.toString(), () -> listener.getInvocationCount() == 1);

         cache1.deregisterCacheEntryListener(conf);
         listener.reset();

         cache2.put(k(m, 2), v(m, 2));
         Eventually.eventually(() -> listener.toString(), () -> listener.getInvocationCount() == 0);
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
      MutableCacheEntryListenerConfiguration conf1 = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener1), null, false, true);
      MutableCacheEntryListenerConfiguration conf2 = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener2), FactoryBuilder.factoryOf(filter), false, true);
      try {
         cache1.registerCacheEntryListener(conf1);
         cache2.registerCacheEntryListener(conf2);
         cache2.put(k(m), v(m, 3));
         Eventually.eventually(() -> listener1.toString(), () -> listener1.getInvocationCount() == 1);
         Eventually.eventually(() -> listener2.toString(), () -> listener2.getInvocationCount() == 0);

         cache1.deregisterCacheEntryListener(conf1);
         listener1.reset();

         cache2.put(k(m, 2), v(m, 2));
         Eventually.eventually(() -> listener1.toString(), () -> listener1.getInvocationCount() == 0);
      } finally {
         cache1.deregisterCacheEntryListener(conf1);
         cache2.deregisterCacheEntryListener(conf2);
      }
   }

   private static class DiscardingCacheEntryEventFilter implements CacheEntryEventFilter, Serializable {

      @Override
      public boolean evaluate(CacheEntryEvent event) throws CacheEntryListenerException {
         return false;
      }
   }

   private static class TestUpdatedListener extends InvocationAwareListener implements CacheEntryUpdatedListener, Serializable {

      @Override
      public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
         Iterator iterator = iterable.iterator();
         while (iterator.hasNext()) {
            addObject(iterator.next());
         }
      }
   }

   private static class TestCreatedListener extends InvocationAwareListener implements CacheEntryCreatedListener, Serializable {

      @Override
      public void onCreated(Iterable iterable) throws CacheEntryListenerException {
         Iterator iterator = iterable.iterator();
         while (iterator.hasNext()) {
            addObject(iterator.next());
         }
      }
   }

   private static class TestRemovedListener extends InvocationAwareListener implements CacheEntryRemovedListener, Serializable {

      @Override
      public void onRemoved(Iterable iterable) throws CacheEntryListenerException {
         Iterator iterator = iterable.iterator();
         while (iterator.hasNext()) {
            addObject(iterator.next());
         }
      }
   }

   private abstract static class InvocationAwareListener {

      protected List<Object> objects = Collections.synchronizedList(new ArrayList<>());

      public synchronized int getInvocationCount() {
         return objects.size();
      }

      public synchronized void reset() {
         objects.clear();
      }

      public synchronized void addObject(Object o) {
         this.objects.add(o);
      }

      @Override
      public String toString() {
         return "InvocationAwareListener{" +
               "objects=" + objects +
               '}';
      }
   }

   private static class CustomEntryProcessor implements EntryProcessor {

      @Override
      public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
         entry.setValue(entry.getValue() + "_processed");
         return entry;
      }
   }

   public abstract Cache getCache1(Method m);
   public abstract Cache getCache2(Method m);
}
