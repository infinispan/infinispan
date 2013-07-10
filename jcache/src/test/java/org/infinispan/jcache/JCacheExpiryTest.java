package org.infinispan.jcache;

import org.infinispan.jcache.util.JCacheRunnable;
import org.testng.annotations.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.spi.CachingProvider;
import java.lang.reflect.Method;

import static org.infinispan.jcache.util.JCacheTestingUtil.withCachingProvider;
import static org.testng.AssertJUnit.*;

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

      cfg.setExpiryPolicyFactory(new Factory<ExpiryPolicy<? super Integer, ? super String>>() {
         @Override
         public ExpiryPolicy<? super Integer, ? super String> create() {
            return new ExpiryPolicy<Integer, String>() {
               @Override
               public Duration getExpiryForCreatedEntry(
                     Cache.Entry<? extends Integer, ? extends String> entry) {
                  return Duration.ETERNAL;
               }

               @Override
               public Duration getExpiryForAccessedEntry(
                     Cache.Entry<? extends Integer, ? extends String> entry) {
                  return null;
               }

               @Override
               public Duration getExpiryForModifiedEntry(
                     Cache.Entry<? extends Integer, ? extends String> entry) {
                  return Duration.ZERO;
               }
            };
         }
      });

      final String name = getName(m);
      withCachingProvider(new JCacheRunnable() {
         @Override
         public void run(CachingProvider provider) {
            CacheManager cm = provider.getCacheManager();
            Cache<Integer, String> cache = cm.configureCache(name, cfg);

            cache.put(1, "v1");
            assertTrue(cache.containsKey(1));
            assertEquals("v1", cache.get(1));

            cache.getAndReplace(1, "v2");
            assertFalse(cache.containsKey(1));
            assertNull(cache.get(1));
         }
      });
   }

   private String getName(Method m) {
      return getClass().getName() + '.' + m.getName();
   }

}
