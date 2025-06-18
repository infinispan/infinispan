package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.stream.Stream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.event.ClusteredListenerMarshallerTest")
public class ClusterNearCacheMarshallingTest extends MultiHotRodServersTest {

   private static final String SERVER_DEFINED_CACHE = "other-cache";
   private Class<? extends Marshaller> marshaller;
   private MediaType storeType;
   private boolean bloomFilter;

   public ClusterNearCacheMarshallingTest() { }

   protected ClusterNearCacheMarshallingTest(Class<? extends Marshaller> marshaller, MediaType storeType, boolean bloomFilter) {
      this.marshaller = marshaller;
      this.storeType = storeType;
      this.bloomFilter = bloomFilter;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder serverCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      if (storeType != null) hotRodCacheConfiguration(serverCfg, storeType);
      createHotRodServers(2, serverCfg);

      waitForClusterToForm();

      org.infinispan.configuration.cache.ConfigurationBuilder cb = new org.infinispan.configuration.cache.ConfigurationBuilder();
      cb.encoding().key().mediaType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
      cb.encoding().value().mediaType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
      TestingUtil.defineConfiguration(manager(0), SERVER_DEFINED_CACHE, cb.build());
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder cb = super.createHotRodClientConfigurationBuilder(host, serverPort);
      if (marshaller != null) cb.marshaller(marshaller);
      cb.remoteCache("").nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(2)
            .nearCacheUseBloomFilter(bloomFilter);
      return cb;
   }

   public void testServerDefinedCache() {
      RemoteCacheManager cacheManager = client(0);
      assertThat(cacheManager.getCache(SERVER_DEFINED_CACHE)).isNotNull();
      assertThat(cacheManager.getCache()).isNotNull();
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
      return Stream.of(true, false)
            .flatMap(bloomFilter -> Stream.of(
                  new ClusterNearCacheMarshallingTest(null, null, bloomFilter),
                  new ClusterNearCacheMarshallingTest(GenericJBossMarshaller.class, MediaType.APPLICATION_JBOSS_MARSHALLING, bloomFilter),
                  new ClusterNearCacheMarshallingTest(ProtoStreamMarshaller.class, MediaType.APPLICATION_PROTOSTREAM, bloomFilter),
                  new ClusterNearCacheMarshallingTest(ProtoStreamMarshaller.class, null, bloomFilter),
                  new ClusterNearCacheMarshallingTest(GenericJBossMarshaller.class, null, bloomFilter),
                  new ClusterNearCacheMarshallingTest(JavaSerializationMarshaller.class, null, bloomFilter)
            ))
            .toArray();
   }

   @Override
   protected String parameters() {
      return String.format("(marshaller=%s, mediaType=%s, bloomFilter=%b", (marshaller != null ? marshaller.getSimpleName() : "null"), storeType, bloomFilter);
   }
}
