package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

/**
 * This test verifies that an entry can be expired from the Hot Rod server
 * using the default expiry lifespan or maxIdle. </p>
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.MixedExpiryTest")
public class MixedExpiryTest extends MultiHotRodServersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      configure(builder);
      createHotRodServers(3, builder);

      ts0 = new ControlledTimeService(0);
      TestingUtil.replaceComponent(manager(0), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService(0);
      TestingUtil.replaceComponent(manager(1), TimeService.class, ts1, true);
      ts2 = new ControlledTimeService(0);
      TestingUtil.replaceComponent(manager(2), TimeService.class, ts2, true);
   }

   protected ControlledTimeService ts0;
   protected ControlledTimeService ts1;
   protected ControlledTimeService ts2;

   protected void configure(ConfigurationBuilder configurationBuilder) {

   }

   public void testMixedExpiryLifespan() {
      RemoteCacheManager client0 = client(0);
      RemoteCache<String, String> remoteCache0 = client0.getCache();

      String key = "someKey";

      assertNull(remoteCache0.put(key, "value1", 1000, TimeUnit.SECONDS, 1000, TimeUnit.SECONDS));
      assertEquals("value1", remoteCache0.get(key)); // expected "value1"
      assertMetadataAndValue(remoteCache0.getWithMetadata(key), "value1", 1000, 1000);
      assertEquals("value1", remoteCache0.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "value2", -1, TimeUnit.SECONDS, 1000,
              TimeUnit.SECONDS));
      assertEquals("value2", remoteCache0.get(key)); // expected "value2"
      assertMetadataAndValue(remoteCache0.getWithMetadata(key), "value2", -1, 1000);
      assertEquals("value2", remoteCache0.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "value3", -1, TimeUnit.SECONDS, 1000,
              TimeUnit.SECONDS));
      assertEquals("value3", remoteCache0.get(key)); // expected "value3"
      assertMetadataAndValue(remoteCache0.getWithMetadata(key), "value3", -1, 1000);
   }

   public void testMixedExpiryMaxIdle() {
      RemoteCacheManager client0 = client(0);
      RemoteCache<String, String> remoteCache0 = client0.getCache();

      String key = "someKey";

      assertNull(remoteCache0.put(key, "value1", 1000, TimeUnit.SECONDS, 1000, TimeUnit.SECONDS));
      assertEquals("value1", remoteCache0.get(key)); // expected "value1"
      assertMetadataAndValue(remoteCache0.getWithMetadata(key), "value1", 1000, 1000);
      assertEquals("value1", remoteCache0.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "value2", 1000, TimeUnit.SECONDS, -1,
              TimeUnit.SECONDS));
      assertEquals("value2", remoteCache0.get(key)); // expected "value2"
      assertMetadataAndValue(remoteCache0.getWithMetadata(key), "value2", 1000, -1);
      assertEquals("value2", remoteCache0.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "value3", 1000, TimeUnit.SECONDS, -1,
              TimeUnit.SECONDS));
      assertEquals("value3", remoteCache0.get(key)); // expected "value3"
      assertMetadataAndValue(remoteCache0.getWithMetadata(key), "value3", 1000, -1);
   }

   public void testMaxIdleRemovedOnAccess() throws InterruptedException, IOException {
      RemoteCacheManager client0 = client(0);
      RemoteCache<String, String> remoteCache0 = client0.getCache();

      String key = "someKey";

      byte[] marshalledKey = client0.getMarshaller().objectToByteBuffer(key);

      Object keyStorage = cache(0).getAdvancedCache().getValueDataConversion().toStorage(marshalledKey);

      assertNull(remoteCache0.put(key, "value1", -1, TimeUnit.MILLISECONDS, 100, TimeUnit.MILLISECONDS));

      for (Cache cache : caches()) {
         DataContainer dataContainer = cache.getAdvancedCache().getDataContainer();
         assertNotNull(dataContainer.peek(keyStorage));
      }

      incrementAllTimeServices(150, TimeUnit.MILLISECONDS);

      assertNull(remoteCache0.get(key));

      for (Cache cache : caches()) {
         DataContainer dataContainer = cache.getAdvancedCache().getDataContainer();
         assertNull(dataContainer.peek(keyStorage));
      }
   }

   private <V> void assertMetadataAndValue(MetadataValue<V> metadataValue, V value, long lifespanSeconds,
           long maxIdleSeconds) {
      assertEquals(value, metadataValue.getValue());
      assertEquals(lifespanSeconds, metadataValue.getLifespan());
      assertEquals(maxIdleSeconds, metadataValue.getMaxIdle());
   }

   private void incrementAllTimeServices(long time, TimeUnit unit) {
      for (ControlledTimeService cts : Arrays.asList(ts0, ts1, ts2)) {
         cts.advance(unit.toMillis(time));
      }
   }
}
