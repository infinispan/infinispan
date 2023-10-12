package org.infinispan.server.resp.types;

import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.infinispan.server.resp.test.RespTestingUtil.startServer;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.TransactionOperationsTest;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.distribution.ch.impl.RESPHashFunctionPartitioner;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.resp.types.TransactionMediaTypeTest")
public class TransactionMediaTypeTest extends TransactionOperationsTest {

   private CacheMode cacheMode;
   private boolean simpleCache;
   private MediaType valueType;

   protected EmbeddedCacheManager createTestCacheManager() {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder = cacheMode == CacheMode.LOCAL
            ? globalBuilder.nonClusteredDefault()
            : globalBuilder.clusteredDefault();
      TestCacheManagerFactory.amendGlobalConfiguration(globalBuilder, new TransportFlags());
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(MediaType.APPLICATION_OCTET_STREAM);
      builder.clustering().hash().keyPartitioner(new RESPHashFunctionPartitioner()).numSegments(256);
      amendConfiguration(builder);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.newDefaultCacheManager(true, globalBuilder, builder);
      TestingUtil.replaceComponent(cacheManager, TimeService.class, timeService, true);
      return cacheManager;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      cacheManager = createTestCacheManager();
      Configuration configuration = defaultRespConfiguration()
            .encoding()
            .value().mediaType(valueType.toString())
            .build();
      RespServerConfiguration serverConfiguration = serverConfiguration().build();
      cacheManager.defineConfiguration(serverConfiguration.defaultCacheName(), configuration);
      server = startServer(cacheManager, serverConfiguration);
      client = createClient(30000, server.getPort());
      redisConnection = client.connect();
      cache = cacheManager.getCache(server.getConfiguration().defaultCacheName());
      return cacheManager;
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      if (simpleCache) {
         configurationBuilder.clustering().cacheMode(CacheMode.LOCAL).simpleCache(true);
      } else {
         configurationBuilder.clustering().cacheMode(cacheMode);
      }
   }

   private TransactionMediaTypeTest withValueType(MediaType type) {
      this.valueType = type;
      return this;
   }

   private TransactionMediaTypeTest withSimpleCache() {
      this.simpleCache = true;
      return this;
   }

   private TransactionMediaTypeTest withCacheMode(CacheMode mode) {
      this.cacheMode = mode;
      return this;
   }

   @Factory
   public Object[] factory() {
      List<TransactionMediaTypeTest> instances = new ArrayList<>();
      MediaType[] types = new MediaType[] {
            MediaType.APPLICATION_OCTET_STREAM,
            MediaType.APPLICATION_PROTOSTREAM,
            MediaType.APPLICATION_OBJECT,
            MediaType.TEXT_PLAIN,
      };
      for (MediaType value : types) {
         instances.add(new TransactionMediaTypeTest().withValueType(value).withCacheMode(CacheMode.LOCAL));
         instances.add(new TransactionMediaTypeTest().withValueType(value).withCacheMode(CacheMode.DIST_SYNC));
         instances.add(new TransactionMediaTypeTest().withValueType(value).withSimpleCache());
      }
      return instances.toArray();
   }

   @Override
   protected String parameters() {
      return "[simpleCache=" + simpleCache + ", cacheMode=" + cacheMode + ", value=" + valueType + "]";
   }
}
