package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.equivalence.AnyServerEquivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * This test verifies that an entry can be expired from the Hot Rod server
 * using the default expiry lifespan or maxIdle. </p>
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.ExpiryTest")
public class MixedExpiryTest extends MultiHotRodServersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.dataContainer().keyEquivalence(new AnyServerEquivalence());
      configure(builder);
      createHotRodServers(1, builder);
   }

   protected void configure(ConfigurationBuilder configurationBuilder) {

   }

   public void testMixedExpiryLifespan() {
      RemoteCacheManager client0 = client(0);
      RemoteCache<String, String> cache0 = client0.getCache();

      String key = "someKey";

      assertNull(cache0.put(key, "value1", 1000, TimeUnit.SECONDS, 1000, TimeUnit.SECONDS));
      assertEquals("value1", cache0.get(key)); // expected "value1"
      assertMetadataAndValue(cache0.getWithMetadata(key), "value1", 1000, 1000);
      assertEquals("value1", cache0.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "value2", -1, TimeUnit.SECONDS, 1000,
              TimeUnit.SECONDS));
      assertEquals("value2", cache0.get(key)); // expected "value2"
      assertMetadataAndValue(cache0.getWithMetadata(key), "value2", -1, 1000);
      assertEquals("value2", cache0.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "value3", -1, TimeUnit.SECONDS, 1000,
              TimeUnit.SECONDS));
      assertEquals("value3", cache0.get(key)); // expected "value3"
      assertMetadataAndValue(cache0.getWithMetadata(key), "value3", -1, 1000);
   }

   public void testMixedExpiryMaxIdle() {
      RemoteCacheManager client0 = client(0);
      RemoteCache<String, String> cache0 = client0.getCache();

      String key = "someKey";

      assertNull(cache0.put(key, "value1", 1000, TimeUnit.SECONDS, 1000, TimeUnit.SECONDS));
      assertEquals("value1", cache0.get(key)); // expected "value1"
      assertMetadataAndValue(cache0.getWithMetadata(key), "value1", 1000, 1000);
      assertEquals("value1", cache0.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "value2", 1000, TimeUnit.SECONDS, -1,
              TimeUnit.SECONDS));
      assertEquals("value2", cache0.get(key)); // expected "value2"
      assertMetadataAndValue(cache0.getWithMetadata(key), "value2", 1000, -1);
      assertEquals("value2", cache0.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "value3", 1000, TimeUnit.SECONDS, -1,
              TimeUnit.SECONDS));
      assertEquals("value3", cache0.get(key)); // expected "value3"
      assertMetadataAndValue(cache0.getWithMetadata(key), "value3", 1000, -1);
   }

   private <V> void assertMetadataAndValue(MetadataValue<V> metadataValue, V value, long lifespanSeconds,
           long maxIdleSeconds) {
      assertEquals(value, metadataValue.getValue());
      assertEquals(lifespanSeconds, metadataValue.getLifespan());
      assertEquals(maxIdleSeconds, metadataValue.getMaxIdle());
   }
}
