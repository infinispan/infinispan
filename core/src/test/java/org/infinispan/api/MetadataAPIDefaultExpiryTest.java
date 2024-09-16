package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ExpirationConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.MetadataAPIDefaultExpiryTest")
public class MetadataAPIDefaultExpiryTest extends SingleCacheManagerTest {

   public static final int EXPIRATION_TIMEOUT = 1000;

   private final ControlledTimeService controlledTimeService = new ControlledTimeService();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.expiration().lifespan(EXPIRATION_TIMEOUT);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(builder);
      TestingUtil.replaceComponent(manager, TimeService.class, controlledTimeService, true);
      return manager;
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
      CompletableFuture<Object> f = cache().putAsync(1, "v1");
      f.get(10, TimeUnit.SECONDS);
      expectCachedThenExpired(1, "v1");
      f = cache().getAdvancedCache().putAsync(2, "v2", new EmbeddedMetadata.Builder().build());
      f.get(10, TimeUnit.SECONDS);
      expectCachedThenExpired(2, "v2");
   }

   public void updateExpiration() {
      cache().put(1, "value");
      CacheEntry<Object, Object> entry = cache().getAdvancedCache().getCacheEntry(1);
      assertEquals(EXPIRATION_TIMEOUT, entry.getLifespan());
      assertEquals(-1, entry.getMaxIdle());
      cache().getCacheConfiguration().expiration().attributes().attribute(ExpirationConfiguration.LIFESPAN).set(TimeQuantity.valueOf(EXPIRATION_TIMEOUT * 2));
      cache().put(2, "value");
      entry = cache().getAdvancedCache().getCacheEntry(2);
      assertEquals(EXPIRATION_TIMEOUT * 2, entry.getLifespan());
      assertEquals(-1, entry.getMaxIdle());
      cache().getCacheConfiguration().expiration().attributes().attribute(ExpirationConfiguration.MAX_IDLE).set(TimeQuantity.valueOf(EXPIRATION_TIMEOUT * 3));
      cache().put(3, "value");
      entry = cache().getAdvancedCache().getCacheEntry(3);
      assertEquals(EXPIRATION_TIMEOUT * 2, entry.getLifespan());
      assertEquals(EXPIRATION_TIMEOUT * 3, entry.getMaxIdle());
   }

   private void expectCachedThenExpired(Integer key, String value) {
      // Check that it doesn't expire too early
      controlledTimeService.advance(EXPIRATION_TIMEOUT - 1);
      String v = this.<Integer, String>cache().get(key);
      assertEquals(value, v);

      // But not too late either
      controlledTimeService.advance(2);
      assertNull(cache.get(key));
   }
}
