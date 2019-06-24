package org.infinispan.client.hotrod;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test proto registration using putAll
 *
 * @since 9.4
 */
@Test(groups = "functional", testName = "client.hotrod.PutAllProtoRegistration")
public class PutAllProtoRegistration extends MultiHotRodServersTest {

   private static final int CLUSTER_SIZE = 2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      createHotRodServers(CLUSTER_SIZE, cfgBuilder);
      waitForClusterToForm();
   }

   @Test
   public void testBatchProtoRegistration() {
      String protoName = "test.proto";
      String protoValue = "message Test {}";
      Map<String, String> value = new HashMap<>();
      value.put(protoName, protoValue);
      RemoteCache<String, String> metadataCache = client(0).getCache(PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.putAll(value);

      String proto = metadataCache.get(protoName);

      assertEquals(proto, protoValue);
   }

}
