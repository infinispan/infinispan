package org.infinispan.jcache;

import org.infinispan.jcache.util.JCacheRunnable;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.spi.CachingProvider;

import static org.infinispan.jcache.JCacheCustomKeyGenerator.CustomGeneratedCacheKey;
import static org.infinispan.jcache.util.JCacheTestingUtil.*;
import static org.junit.Assert.*;

/**
 * Base class for jsr107 caching annotations tests. Implementations must provide cache & {@link org.infinispan.jcache.JCacheAnnotatedClass} references.
 *
 * @author Matej Cimbora
 */
//TODO Test exception handling once implemented (e.g. cacheFor, evictFor, etc.)
public abstract class AbstractTwoCachesAnnotationsTest {

   @Test
   public void testPut() {
      withCachingProvider(new JCacheRunnable() {
         @Override
         public void run(CachingProvider provider) {
            Cache cache1 = getCache1(provider);
            Cache cache2 = getCache2(provider);

            getJCacheAnnotatedClass().put("val");
            assertEquals(1, getEntryCount(cache1.iterator()));
            assertEquals(1, getEntryCount(cache2.iterator()));
            assertTrue(cache1.containsKey(new CustomGeneratedCacheKey("val")));
            assertTrue(cache2.containsKey(new CustomGeneratedCacheKey("val")));
         }
      });
   }

   @Test
   public void testResult()  {
      withCachingProvider(new JCacheRunnable() {
         @Override
         public void run(CachingProvider provider) {
            Cache cache1 = getCache1(provider);
            Cache cache2 = getCache2(provider);

            assertEquals(0, getJCacheAnnotatedClass().getResultInvocationCount());

            getJCacheAnnotatedClass().result("val");
            assertEquals(1, getJCacheAnnotatedClass().getResultInvocationCount());

            getJCacheAnnotatedClass().result("val");
            assertEquals(1, getJCacheAnnotatedClass().getResultInvocationCount());

            assertEquals(1, getEntryCount(cache1.iterator()));
            assertEquals(1, getEntryCount(cache2.iterator()));
            assertTrue(cache1.containsKey(new CustomGeneratedCacheKey("val")));
            assertTrue(cache2.containsKey(new CustomGeneratedCacheKey("val")));
         }
      });
   }

   @Test
   public void testRemove() {
      withCachingProvider(new JCacheRunnable() {
         @Override
         public void run(CachingProvider provider) {
            Cache cache1 = getCache1(provider);
            Cache cache2 = getCache2(provider);

            cache1.put("key1", "val1");
            assertTrue(cache1.containsKey("key1"));
            assertTrue(cache2.containsKey("key1"));

            getJCacheAnnotatedClass().remove("key1");
            assertFalse(cache1.containsKey(new CustomGeneratedCacheKey("key1")));
            assertFalse(cache2.containsKey(new CustomGeneratedCacheKey("key1")));
         }
      });
   }

   @Test
   public void testRemoveAll() {
      withCachingProvider(new JCacheRunnable() {
         @Override
         public void run(CachingProvider provider) {
            Cache cache1 = getCache1(provider);
            Cache cache2 = getCache2(provider);

            cache1.put("key1", "val1");
            cache1.put("key2", "val2");
            assertTrue(cache1.containsKey("key1"));
            assertTrue(cache2.containsKey("key1"));
            assertTrue(cache1.containsKey("key2"));
            assertTrue(cache2.containsKey("key2"));

            getJCacheAnnotatedClass().removeAll();
            assertFalse(cache1.containsKey(new CustomGeneratedCacheKey("key1")));
            assertFalse(cache1.containsKey(new CustomGeneratedCacheKey("key2")));
            assertFalse(cache2.containsKey(new CustomGeneratedCacheKey("key1")));
            assertFalse(cache2.containsKey(new CustomGeneratedCacheKey("key2")));
         }
      });
   }

   public abstract JCacheAnnotatedClass getJCacheAnnotatedClass();
   public abstract Cache getCache1(CachingProvider provider);
   public abstract Cache getCache2(CachingProvider provider);
}
