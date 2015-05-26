package org.infinispan.api;

import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

@Test(groups = "functional", testName = "api.MetadataAPIDefaultExpiryTest")
public class MetadataAPIDefaultExpiryTest extends SingleCacheManagerTest {

   public static final int EXPIRATION_TIMEOUT = 3000;
   public static final int EVICTION_CHECK_TIMEOUT = 2000;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.expiration().lifespan(EXPIRATION_TIMEOUT);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testDefaultLifespanPut() {
      cache().put(1, "v1");
      expectCachedThenExpired(1, "v1");
      cache().getAdvancedCache().put(2, "v2", new EmbeddedMetadata.Builder().build());
      expectCachedThenExpired(2, "v2");
   }

   public void testDefaultLifespanReplace() {
      cache().put(1, "v1");
      cache().replace(1, "v11");
      expectCachedThenExpired(1, "v11");
      cache().getAdvancedCache().put(2, "v2", new EmbeddedMetadata.Builder().build());
      cache().getAdvancedCache().replace(2, "v22", new EmbeddedMetadata.Builder().build());
      expectCachedThenExpired(2, "v22");
   }

   public void testDefaultLifespanReplaceWithOldValue() {
      cache().put(1, "v1");
      cache().replace(1, "v1", "v11");
      expectCachedThenExpired(1, "v11");
      cache().getAdvancedCache().put(2, "v2", new EmbeddedMetadata.Builder().build());
      cache().getAdvancedCache().replace(2, "v2", "v22", new EmbeddedMetadata.Builder().build());
      expectCachedThenExpired(2, "v22");
   }

   public void testDefaultLifespanPutIfAbsent() {
      cache().putIfAbsent(1, "v1");
      expectCachedThenExpired(1, "v1");
      cache().getAdvancedCache().putIfAbsent(2, "v2", new EmbeddedMetadata.Builder().build());
      expectCachedThenExpired(2, "v2");
   }

   public void testDefaultLifespanPutForExternalRead() {
      cache().putForExternalRead(1, "v1");
      expectCachedThenExpired(1, "v1");
      cache().getAdvancedCache().putForExternalRead(2, "v2", new EmbeddedMetadata.Builder().build());
      expectCachedThenExpired(2, "v2");
   }

   public void testDefaultLifespanPutAsync() throws Exception {
      NotifyingFuture<Object> f = cache().putAsync(1, "v1");
      f.get(10, TimeUnit.SECONDS);
      expectCachedThenExpired(1, "v1");
      f = cache().getAdvancedCache().putAsync(2, "v2", new EmbeddedMetadata.Builder().build());
      f.get(10, TimeUnit.SECONDS);
      expectCachedThenExpired(2, "v2");
   }

   private void expectCachedThenExpired(Integer key, String value) {
      final long startTime = now();
      final long expiration = EXPIRATION_TIMEOUT;
      while (true) {
         String v = this.<Integer, String>cache().get(key);
         if (moreThanDurationElapsed(startTime, expiration))
            break;
         assertEquals(value, v);
         sleepThread(100);
      }

      // Make sure that in the next X secs data is removed
      while (!moreThanDurationElapsed(startTime, expiration + EVICTION_CHECK_TIMEOUT)) {
         if (cache.get(key) == null) return;
      }

      assertNull(cache.get(key));
   }
}
