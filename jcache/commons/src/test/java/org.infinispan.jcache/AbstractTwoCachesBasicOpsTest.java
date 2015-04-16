package org.infinispan.jcache;

import org.junit.Test;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.infinispan.jcache.util.JCacheTestingUtil.getEntryCount;
import static org.infinispan.jcache.util.JCacheTestingUtil.sleep;
import static org.junit.Assert.*;

/**
 * Base class for jsr107 cache tests. Implementations must provide cache references.
 *
 * @author Matej Cimbora
 */
public abstract class AbstractTwoCachesBasicOpsTest {

   @Test
   public void testCacheManagerXmlConfig() {
      Cache<String, String> cache = getCache1();

      cache.put("key1", "val1");
      assertEquals("val1", cache.get("key1"));
   }

   @Test
   public void testMultipleCacheManagersXmlConfig() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      assertEquals("val1", cache1.get("key1"));
      assertEquals("val1", cache2.get("key1"));
   }

   @Test
   public void testRemove() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      assertTrue(cache1.containsKey("key1"));
      assertTrue(cache2.containsKey("key1"));
      cache2.remove("key1");
      assertFalse(cache1.containsKey("key1"));
      assertFalse(cache2.containsKey("key1"));
   }

   @Test
   public void testRemoveAll() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      cache1.put("key2", "val2");
      assertTrue(cache1.containsKey("key1"));
      assertTrue(cache1.containsKey("key2"));
      assertTrue(cache2.containsKey("key1"));
      assertTrue(cache2.containsKey("key2"));

      Set<String> keySet = new HashSet<>();
      keySet.add("key1");
      cache2.removeAll(keySet);
      assertFalse(cache1.containsKey("key1"));
      assertTrue(cache1.containsKey("key2"));
      assertFalse(cache2.containsKey("key1"));
      assertTrue(cache2.containsKey("key2"));

      cache1.removeAll();
      assertFalse(cache1.containsKey("key2"));
      assertFalse(cache2.containsKey("key2"));
   }

   @Test
   public void testPutAll() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      Map<String, String> entryMap = new HashMap<>();
      entryMap.put("key1", "val1");
      entryMap.put("key2", "val2");
      cache1.putAll(entryMap);
      assert cache1.containsKey("key1");
      assert cache1.containsKey("key2");
      assert cache2.containsKey("key1");
      assert cache2.containsKey("key2");
   }

   @Test
   public void testPutIfAbsent() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      cache2.putIfAbsent("key1", "val3");
      assertEquals("val1", cache1.get("key1"));
      assertEquals("val1", cache2.get("key1"));

      cache1.putIfAbsent("key2", "val2");
      assertTrue(cache1.containsKey("key2"));
      assertTrue(cache2.containsKey("key2"));
   }

   @Test
   public void testGetAndPut() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      String result = cache2.getAndPut("key1", "val2");
      assertEquals("val1", result);
      assertEquals("val2", cache1.get("key1"));
      assertEquals("val2", cache2.get("key1"));
   }

   @Test
   public void testGetAndRemove() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      String result = cache2.getAndRemove("key1");
      assertEquals("val1", result);
      assertFalse(cache1.containsKey("key1"));
      assertFalse(cache2.containsKey("key1"));
   }

   @Test
   public void testGetAndReplace() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      String result = cache2.getAndReplace("key1", "val2");
      assertEquals(result, "val1");
      assertEquals("val2", cache1.get("key1"));
      assertEquals("val2", cache2.get("key1"));
   }

   @Test
   public void testGetAll() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      cache1.put("key2", "val2");
      Set<String> keySet = new HashSet<>();
      keySet.add("key1");
      keySet.add("key2");
      Map<String, String> result = cache2.getAll(keySet);
      assertEquals(2, result.size());
      assertTrue(result.containsKey("key1"));
      assertTrue(result.containsKey("key2"));
      result = cache1.getAll(keySet);
      assertEquals(2, result.size());
      assertTrue(result.containsKey("key1"));
      assertTrue(result.containsKey("key2"));
   }

   @Test
   public void testReplace() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      assertTrue(cache1.containsKey("key1"));
      assertTrue(cache2.containsKey("key1"));

      cache2.replace("key1", "val2");
      assertEquals("val2", cache1.get("key1"));
      assertEquals("val2", cache2.get("key1"));

      cache1.replace("key1", "val2", "val3");
      assertEquals("val3", cache1.get("key1"));
      assertEquals("val3", cache2.get("key1"));

      boolean result = cache2.replace("key1", "staleValue", "val4");
      assertFalse(result);
      assertEquals("val3", cache1.get("key1"));
      assertEquals("val3", cache2.get("key1"));
   }

   @Test
   public void testIterator() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      for (int i = 0; i < 5; i++) {
         cache1.put(String.valueOf(i), "val");
      }
      assertEquals(5, getEntryCount(cache1.iterator()));
      assertEquals(5, getEntryCount(cache2.iterator()));
   }

   @Test
   public void testClusteredClear() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      cache1.put("key2", "val2");
      cache2.clear();
      assertNull(cache1.get("key1"));
      assertNull(cache2.get("key1"));
      assertNull(cache1.get("key2"));
      assertNull(cache2.get("key2"));
   }

   @Test
   public void testEntryProcessor() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      cache1.put("key1", "val1");
      cache2.invoke("key1", new CustomEntryProcessor(), null);
      assertEquals("val1_processed", cache1.get("key1"));
      assertEquals("val1_processed", cache2.get("key1"));
   }

   @Test
   public void testUpdatedListener() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      TestUpdatedListener listener = new TestUpdatedListener();
      MutableCacheEntryListenerConfiguration conf = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener), null, false, false);
      cache1.registerCacheEntryListener(conf);
      cache1.put("key1", "val2");
      cache2.put("key1", "val3");
      sleep(1000);
      assertEquals(1, listener.getInvocationCount());

      listener.reset();
      cache1.deregisterCacheEntryListener(conf);
      cache2.put("key1", "val4");
      sleep(1000);
      assertEquals(0, listener.getInvocationCount());
   }

   @Test
   public void testRemovedListener() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      TestRemovedListener listener = new TestRemovedListener();
      MutableCacheEntryListenerConfiguration conf = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener), null, false, false);
      cache1.registerCacheEntryListener(conf);
      cache1.put("key1", "val3");
      cache2.remove("key1");
      sleep(1000);
      assertEquals(1, listener.getInvocationCount());

      listener.reset();
      cache1.deregisterCacheEntryListener(conf);
      cache1.put("key2", "val2");
      cache2.remove("key2");
      sleep(1000);
      assertEquals(0, listener.getInvocationCount());
   }

   @Test
   public void testCreatedListener() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      TestCreatedListener listener = new TestCreatedListener();
      MutableCacheEntryListenerConfiguration conf = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener), null, false, false);
      cache1.registerCacheEntryListener(conf);
      cache2.put("key1", "val3");
      sleep(1000);
      assertEquals(1, listener.getInvocationCount());

      listener.reset();
      cache1.deregisterCacheEntryListener(conf);
      cache2.put("key2", "val2");
      sleep(1000);
      assertEquals(0, listener.getInvocationCount());
   }

   @Test
   public void testListenerDiscardingFilter() {
      Cache<String, String> cache1 = getCache1();
      Cache<String, String> cache2 = getCache2();

      DiscardingCacheEntryEventFilter filter = new DiscardingCacheEntryEventFilter();
      TestCreatedListener listener1 = new TestCreatedListener();
      TestCreatedListener listener2 = new TestCreatedListener();
      MutableCacheEntryListenerConfiguration conf1 = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener1), null, false, false);
      MutableCacheEntryListenerConfiguration conf2 = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener2), FactoryBuilder.factoryOf(filter), false, false);
      cache1.registerCacheEntryListener(conf1);
      cache2.registerCacheEntryListener(conf2);
      cache2.put("key1", "val3");
      sleep(1000);
      assertEquals(1, listener1.getInvocationCount());
      assertEquals(0, listener2.getInvocationCount());

      listener1.reset();
      cache1.deregisterCacheEntryListener(conf1);
      cache2.put("key2", "val2");
      sleep(1000);
      assertEquals(0, listener1.getInvocationCount());
   }

   @Test
   public void testExpiration() {
      Cache<String, String> cache1 = getExpiryCache1();
      Cache<String, String> cache2 = getExpiryCache2();

      TestExpiredListener listener = new TestExpiredListener();
      MutableCacheEntryListenerConfiguration conf1 = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener), null, false, false);
      cache1.registerCacheEntryListener(conf1);
      cache2.put("key1", "val1");
      sleep(5000);
      assert cache1.get("key1") == null;
      assertEquals(1, listener.getInvocationCount());

      listener.reset();
      cache1.deregisterCacheEntryListener(conf1);
      cache2.put("key2", "val2");
      sleep(5000);
      assert cache1.get("key2") == null;
      assertEquals(0, listener.getInvocationCount());
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
            iterator.next();
            invocationCount++;
         }
      }
   }

   private static class TestCreatedListener extends InvocationAwareListener implements CacheEntryCreatedListener, Serializable {

      @Override
      public void onCreated(Iterable iterable) throws CacheEntryListenerException {
         Iterator iterator = iterable.iterator();
         while (iterator.hasNext()) {
            iterator.next();
            invocationCount++;
         }
      }
   }

   private static class TestRemovedListener extends InvocationAwareListener implements CacheEntryRemovedListener, Serializable {

      @Override
      public void onRemoved(Iterable iterable) throws CacheEntryListenerException {
         Iterator iterator = iterable.iterator();
         while (iterator.hasNext()) {
            iterator.next();
            invocationCount++;
         }
      }
   }

   private static class TestExpiredListener extends InvocationAwareListener implements CacheEntryExpiredListener, Serializable {

      @Override
      public void onExpired(Iterable iterable) throws CacheEntryListenerException {
         Iterator iterator = iterable.iterator();
         while (iterator.hasNext()) {
            iterator.next();
            invocationCount++;
         }
      }
   }

   private abstract static class InvocationAwareListener {

      protected int invocationCount;

      public int getInvocationCount() {
         return invocationCount;
      }

      public void reset() {
         invocationCount = 0;
      }
   }

   private static class CustomEntryProcessor implements EntryProcessor {

      @Override
      public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
         entry.setValue(entry.getValue() + "_processed");
         return entry;
      }
   }

   public abstract Cache getCache1();
   public abstract Cache getCache2();
   public abstract Cache getExpiryCache1();
   public abstract Cache getExpiryCache2();
}
