package org.infinispan.jcache;

import org.jboss.arquillian.testng.Arquillian;
import org.testng.annotations.Test;

import javax.cache.Cache;
import java.lang.reflect.Method;

import static org.infinispan.jcache.JCacheCustomKeyGenerator.CustomGeneratedCacheKey;
import static org.infinispan.jcache.util.JCacheTestingUtil.getEntryCount;
import static org.testng.Assert.*;

/**
 * Base class for clustered JCache annotations tests. Implementations must provide cache & {@link
 * org.infinispan.jcache.JCacheAnnotatedClass} references.
 *
 * @author Matej Cimbora
 */
//TODO Test exception handling once implemented (e.g. cacheFor, evictFor, etc.)
@Test(testName = "org.infinispan.jcache.AbstractTwoCachesAnnotationsTest", groups = "functional")
public abstract class AbstractTwoCachesAnnotationsTest extends Arquillian {

   @Test
   public void testPut(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      getJCacheAnnotatedClass().put("val");
      assertEquals(getEntryCount(cache1.iterator()), 1);
      assertEquals(getEntryCount(cache2.iterator()), 1);
      assertTrue(cache1.containsKey(new CustomGeneratedCacheKey("val")));
      assertTrue(cache2.containsKey(new CustomGeneratedCacheKey("val")));
   }

   @Test
   public void testResult(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      assertEquals(getJCacheAnnotatedClass().getResultInvocationCount(), 0);

      getJCacheAnnotatedClass().result("val");
      assertEquals(getJCacheAnnotatedClass().getResultInvocationCount(), 1);

      getJCacheAnnotatedClass().result("val");
      assertEquals(getJCacheAnnotatedClass().getResultInvocationCount(), 1);

      assertEquals(getEntryCount(cache1.iterator()), 1);
      assertEquals(getEntryCount(cache2.iterator()), 1);
      assertTrue(cache1.containsKey(new CustomGeneratedCacheKey("val")));
      assertTrue(cache2.containsKey(new CustomGeneratedCacheKey("val")));
   }

   @Test
   public void testRemove(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      cache1.put("key1", "val1");
      assertTrue(cache1.containsKey("key1"));
      assertTrue(cache2.containsKey("key1"));

      getJCacheAnnotatedClass().remove("key1");
      assertFalse(cache1.containsKey(new CustomGeneratedCacheKey("key1")));
      assertFalse(cache2.containsKey(new CustomGeneratedCacheKey("key1")));
   }

   @Test
   public void testRemoveAll(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

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

   public abstract JCacheAnnotatedClass getJCacheAnnotatedClass();
   public abstract Cache getCache1(Method m);
   public abstract Cache getCache2(Method m);
}
