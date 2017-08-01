package org.infinispan.jcache;

import static org.infinispan.jcache.JCacheCustomKeyGenerator.CustomGeneratedCacheKey;
import static org.infinispan.jcache.util.JCacheTestingUtil.getEntryCount;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;

import javax.cache.Cache;

import org.infinispan.test.fwk.TestResourceTrackingListener;
import org.jboss.arquillian.testng.Arquillian;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

/**
 * Base class for clustered JCache annotations tests. Implementations must provide cache & {@link
 * org.infinispan.jcache.JCacheAnnotatedClass} references.
 *
 * @author Matej Cimbora
 */
//TODO Test exception handling once implemented (e.g. cacheFor, evictFor, etc.)
@Listeners(TestResourceTrackingListener.class)
@Test(testName = "org.infinispan.jcache.AbstractTwoCachesAnnotationsTest", groups = "functional")
public abstract class AbstractTwoCachesAnnotationsTest extends Arquillian {

   @Test
   public void testPut(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      getJCacheAnnotatedClass().put(v(m));
      assertEquals(getEntryCount(cache1.iterator()), 1);
      assertEquals(getEntryCount(cache2.iterator()), 1);
      assertTrue(cache1.containsKey(new CustomGeneratedCacheKey(v(m))));
      assertTrue(cache2.containsKey(new CustomGeneratedCacheKey(v(m))));
   }

   @Test
   public void testResult(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      assertEquals(getJCacheAnnotatedClass().getResultInvocationCount(), 0);

      getJCacheAnnotatedClass().result(v(m));
      assertEquals(getJCacheAnnotatedClass().getResultInvocationCount(), 1);

      getJCacheAnnotatedClass().result(v(m));
      assertEquals(getJCacheAnnotatedClass().getResultInvocationCount(), 1);

      assertEquals(getEntryCount(cache1.iterator()), 1);
      assertEquals(getEntryCount(cache2.iterator()), 1);
      assertTrue(cache1.containsKey(new CustomGeneratedCacheKey(v(m))));
      assertTrue(cache2.containsKey(new CustomGeneratedCacheKey(v(m))));
   }

   @Test
   public void testRemove(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      assertTrue(cache1.containsKey(k(m)));
      assertTrue(cache2.containsKey(k(m)));

      getJCacheAnnotatedClass().remove(k(m));
      assertFalse(cache1.containsKey(new CustomGeneratedCacheKey(k(m))));
      assertFalse(cache2.containsKey(new CustomGeneratedCacheKey(k(m))));
   }

   @Test
   public void testRemoveAll(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      cache1.put(k(m), v(m));
      cache1.put(k(m, 2), v(m, 2));
      assertTrue(cache1.containsKey(k(m)));
      assertTrue(cache2.containsKey(k(m)));
      assertTrue(cache1.containsKey(k(m, 2)));
      assertTrue(cache2.containsKey(k(m, 2)));

      getJCacheAnnotatedClass().removeAll();
      assertFalse(cache1.containsKey(new CustomGeneratedCacheKey(k(m))));
      assertFalse(cache1.containsKey(new CustomGeneratedCacheKey(k(m, 2))));
      assertFalse(cache2.containsKey(new CustomGeneratedCacheKey(k(m))));
      assertFalse(cache2.containsKey(new CustomGeneratedCacheKey(k(m, 2))));
   }

   public abstract JCacheAnnotatedClass getJCacheAnnotatedClass();
   public abstract Cache getCache1(Method m);
   public abstract Cache getCache2(Method m);
}
