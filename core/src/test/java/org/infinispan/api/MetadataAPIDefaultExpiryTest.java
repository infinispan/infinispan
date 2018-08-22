package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
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
