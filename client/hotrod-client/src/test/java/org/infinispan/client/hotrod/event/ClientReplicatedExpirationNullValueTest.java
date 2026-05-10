package org.infinispan.client.hotrod.event;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.encoding.DataConversion;
import org.infinispan.expiration.impl.ExpiredCacheListener;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Reproducer for https://github.com/infinispan/infinispan/issues/17527
 *
 * When an entry with an empty protobuf value (byte[0]) expires via max-idle in a
 * replicated SYNC cache, the ProtostreamTranscoder throws NPE while transcoding
 * the value for a listener notification. This kills the JGroups thread on the
 * backup node, causing the primary to hang waiting for a response.
 */
@Test(groups = "functional", testName = "client.hotrod.event.ClientReplicatedExpirationNullValueTest")
public class ClientReplicatedExpirationNullValueTest extends MultiHotRodServersTest {

   private static final int NUM_SERVERS = 2;

   private ControlledTimeService ts0;
   private ControlledTimeService ts1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.expiration().disableReaper();
      createHotRodServers(NUM_SERVERS, hotRodCacheConfiguration(builder));
      ts0 = new ControlledTimeService();
      TestingUtil.replaceComponent(server(0).getCacheManager(), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService();
      TestingUtil.replaceComponent(server(1).getCacheManager(), TimeService.class, ts1, true);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return ClientEventSCI.INSTANCE;
   }

   public void testNullValueTranscoding() {
      // Verify that transcoding empty protobuf bytes (which unmarshal to null)
      // from APPLICATION_PROTOSTREAM to APPLICATION_OCTET_STREAM does not throw NPE.
      // Without the fix, getCtxForMarshalling(null) throws NullPointerException.
      AdvancedCache<?, ?> transcodingCache = cache(0).getAdvancedCache()
            .withMediaType(APPLICATION_PROTOSTREAM, APPLICATION_OCTET_STREAM);
      DataConversion valueConversion = transcodingCache.getValueDataConversion();
      Object result = valueConversion.fromStorage(new byte[0]);
      assertNull(result);
   }

   public void testNullValueMaxIdleExpiration() {
      ExpiredCacheListener listener = new ExpiredCacheListener();
      cache(0).getAdvancedCache().withMediaType(APPLICATION_PROTOSTREAM, APPLICATION_OCTET_STREAM)
            .addListener(listener);

      @SuppressWarnings("unchecked")
      AdvancedCache<byte[], byte[]> rawCache =
            (AdvancedCache<byte[], byte[]>) (AdvancedCache<?, ?>)
            cache(0).getAdvancedCache().withStorageMediaType();
      byte[] keyBytes = new byte[]{1, 2, 3, 4};
      rawCache.put(keyBytes, new byte[0], -1, TimeUnit.MILLISECONDS, 10, TimeUnit.MINUTES);

      ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
      ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

      // Accessing the expired entry triggers max-idle expiration.
      // Without the fix, the NPE in ProtostreamTranscoder.getCtxForMarshalling
      // could kill a JGroups thread on the backup, causing the primary to hang.
      assertNull(rawCache.get(keyBytes));
      eventually(() -> listener.getInvocationCount() > 0, 10_000);
   }
}
