package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.stream.Stream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.NearCacheMarshallingTest")
public class NearCacheMarshallingTest extends SingleHotRodServerTest {

   private static final String SERVER_DEFINED_CACHE = "other-cache";
   private final Class<? extends Marshaller> marshaller;
   private final MediaType storeType;
   private final boolean useBloomFilter;

   protected NearCacheMarshallingTest(Class<? extends Marshaller> marshaller, MediaType storeType, boolean useBloomFilter) {
      this.marshaller = marshaller;
      this.storeType = storeType;
      this.useBloomFilter = useBloomFilter;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder serverCfg = new org.infinispan.configuration.cache.ConfigurationBuilder();
      if (storeType != null) hotRodCacheConfiguration(serverCfg, storeType);
      EmbeddedCacheManager ecm = TestCacheManagerFactory.createCacheManager(contextInitializer(), serverCfg);
      org.infinispan.configuration.cache.ConfigurationBuilder cb = new org.infinispan.configuration.cache.ConfigurationBuilder();
      cb.encoding().key().mediaType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
      cb.encoding().value().mediaType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
      TestingUtil.defineConfiguration(ecm, SERVER_DEFINED_CACHE, cb.build());
      return ecm;
   }

   @Override
   protected void setup() throws Exception {
      cacheManager = createCacheManager();
      hotrodServer = createHotRodServer();
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      if (marshaller != null) builder.marshaller(marshaller);
      builder.remoteCache("").nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(2)
            .nearCacheUseBloomFilter(useBloomFilter);
      return new RemoteCacheManager(builder.build());
   }

   public void testServerDefinedCache() {
      RemoteCacheManager cacheManager = getRemoteCacheManager();

      assertThat(cacheManager.getCache(SERVER_DEFINED_CACHE)).isNotNull();
      assertThat(cacheManager.getCache()).isNotNull();

      cacheManager.stop();
   }

   public void testRemoteWriteOnLocal() {
      RemoteCacheManager cacheManager = getRemoteCacheManager();
      RemoteCacheManager cacheManager1 = getRemoteCacheManager();

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
      eventuallyEquals("V1", () -> cache.get("K"));

      cacheManager.stop();
      cacheManager1.stop();
   }

   @Factory
   protected static Object[] testInstances() {
      return Stream.of(true, false)
            .flatMap(useBloomFilter ->
               Stream.of(
                     new NearCacheMarshallingTest(null, null, useBloomFilter), // Let default.
                     new NearCacheMarshallingTest(GenericJBossMarshaller.class, MediaType.APPLICATION_JBOSS_MARSHALLING, useBloomFilter),
                     new NearCacheMarshallingTest(ProtoStreamMarshaller.class, MediaType.APPLICATION_PROTOSTREAM, useBloomFilter),
                     new NearCacheMarshallingTest(ProtoStreamMarshaller.class, null, useBloomFilter),
                     new NearCacheMarshallingTest(GenericJBossMarshaller.class, null, useBloomFilter),
                     new NearCacheMarshallingTest(JavaSerializationMarshaller.class, null, useBloomFilter)
               ))
            .toArray();
   }

   @Override
   protected String parameters() {
      return String.format("(marshaller=%s, mediaType=%s, bloomFilter=%b", (marshaller != null ? marshaller.getSimpleName() : "null"), storeType, useBloomFilter);
   }
}
