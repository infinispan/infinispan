package org.infinispan.jcache;

import static org.infinispan.jcache.util.JCacheTestingUtil.withCachingProvider;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

import org.testng.annotations.Test;

/**
 * JCache expiry tests not covered by the TCK.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "jcache.JCacheExpiryTest")
public class JCacheExpiryTest {

   public void testGetAndReplace(Method m) {
      final MutableConfiguration<Integer, String>
            cfg = new MutableConfiguration<Integer, String>();

      cfg.setExpiryPolicyFactory((Factory<ExpiryPolicy>) () -> new ExpiryPolicy() {
         @Override
         public Duration getExpiryForCreation() {
            return Duration.ETERNAL;
         }

         @Override
         public Duration getExpiryForAccess() {
            return null;
         }

         @Override
         public Duration getExpiryForUpdate() {
            return Duration.ZERO;
         }
      });

      final String name = getName(m);
      withCachingProvider(provider -> {
         CacheManager cm = provider.getCacheManager();
         Cache<Integer, String> cache = cm.createCache(name, cfg);

         cache.put(1, "v1");
         assertTrue(cache.containsKey(1));
         assertEquals("v1", cache.get(1));

         cache.getAndReplace(1, "v2");
         assertFalse(cache.containsKey(1));
         assertNull(cache.get(1));
      });
   }

   private String getName(Method m) {
      return getClass().getName() + '.' + m.getName();
   }

}
