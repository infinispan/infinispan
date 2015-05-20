package org.infinispan.jcache;

import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Iterator;

import static org.infinispan.jcache.util.JCacheTestingUtil.sleep;
import static org.testng.Assert.*;

/**
 * Base class for clustered JCache expiration tests. Implementations must provide cache references.
 *
 * @author Matej Cimbora
 */
@Test(testName = "org.infinispan.jcache.AbstractTwoCachesExpirationTest", groups = "functional")
public abstract class AbstractTwoCachesExpirationTest extends MultipleCacheManagersTest {

   @Test
   public void testExpiration(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      TestExpiredListener listener = new TestExpiredListener();
      MutableCacheEntryListenerConfiguration conf1 = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener), null, false, false);
      cache1.registerCacheEntryListener(conf1);
      cache2.put("key1", "val1");
      sleep(5000);
      assertNull(cache1.get("key1"));
      assertEquals(listener.invocationCount, 1);

      listener.invocationCount = 0;
      cache1.deregisterCacheEntryListener(conf1);
      cache2.put("key2", "val2");
      sleep(5000);
      assertNull(cache1.get("key2"));
      assertEquals(listener.invocationCount, 0);
   }

   private static class TestExpiredListener implements CacheEntryExpiredListener, Serializable {

      private int invocationCount;

      @Override
      public void onExpired(Iterable iterable) throws CacheEntryListenerException {
         Iterator iterator = iterable.iterator();
         while (iterator.hasNext()) {
            iterator.next();
            invocationCount++;
         }
      }
   }

   public abstract Cache getCache1(Method m);
   public abstract Cache getCache2(Method m);
}
