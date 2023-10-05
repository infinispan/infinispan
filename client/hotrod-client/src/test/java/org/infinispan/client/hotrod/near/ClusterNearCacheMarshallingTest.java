package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.event.ClusteredListenerMarshallerTest")
public class ClusterNearCacheMarshallingTest extends MultiHotRodServersTest {

   private Class<? extends Marshaller> marshaller;
   private MediaType storeType;

   public ClusterNearCacheMarshallingTest() { }

   protected ClusterNearCacheMarshallingTest(Class<? extends Marshaller> marshaller, MediaType storeType) {
      this.marshaller = marshaller;
      this.storeType = storeType;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder serverCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      if (storeType != null) hotRodCacheConfiguration(serverCfg, storeType);
      createHotRodServers(2, serverCfg);

      waitForClusterToForm();
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder cb = super.createHotRodClientConfigurationBuilder(host, serverPort);
      if (marshaller != null) cb.marshaller(marshaller);
      cb.remoteCache("").nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(2)
            .nearCacheUseBloomFilter(true);
      cb.connectionPool().maxActive(1);
      return cb;
   }

   public void testRemoteWriteOnLocal() {
      RemoteCacheManager cacheManager = client(0);
      RemoteCacheManager cacheManager1 = client(1);

      RemoteCache<String, String> cache = cacheManager.getCache();
      cache.put("K", "V");
      assertThat(cache.get("K")).isEqualTo("V");

      RemoteCache<String, String> cache1 = cacheManager1.getCache();
      assertThat(cache1.get("K")).isEqualTo("V");

      // Another client updates the value.
      cache1.replace("K", "V1");

      // Take effect immediately.
      assertThat(cache1.get("K")).isEqualTo("V1");

      // The other cache eventually updates to reflect the replace.
      eventually(() -> cache.get("K").equals("V1"));
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new ClusterNearCacheMarshallingTest(null, null),
            new ClusterNearCacheMarshallingTest(GenericJBossMarshaller.class, MediaType.APPLICATION_JBOSS_MARSHALLING),
            new ClusterNearCacheMarshallingTest(ProtoStreamMarshaller.class, MediaType.APPLICATION_PROTOSTREAM),
            new ClusterNearCacheMarshallingTest(ProtoStreamMarshaller.class, null),
            new ClusterNearCacheMarshallingTest(GenericJBossMarshaller.class, null),
      };
   }

   @Override
   protected String parameters() {
      return String.format("(marshaller=%s, mediaType=%s", (marshaller != null ? marshaller.getSimpleName() : "null"), storeType);
   }
}
