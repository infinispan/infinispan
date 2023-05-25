package org.infinispan.server.resp;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.infinispan.server.resp.test.RespTestingUtil.startServer;

@Test(groups = "functional", testName = "server.resp.RespMediaTypesTest")
public class RespMediaTypesTest extends RespSingleNodeTest {

   private MediaType keyType;
   private MediaType valueType;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      cacheManager = createTestCacheManager();
      Configuration configuration = new ConfigurationBuilder()
            .encoding()
            .key().mediaType(keyType.toString())
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

   private RespMediaTypesTest withKeyType(MediaType type) {
      this.keyType = type;
      return this;
   }

   private RespMediaTypesTest withValueType(MediaType type) {
      this.valueType = type;
      return this;
   }

   @Factory
   public Object[] factory() {
      List<RespMediaTypesTest> instances = new ArrayList<>();
      MediaType[] types = new MediaType[] {
            MediaType.APPLICATION_OCTET_STREAM,
            MediaType.APPLICATION_OBJECT,
            MediaType.TEXT_PLAIN,
      };
      for (MediaType key : types) {
         for (MediaType value : types) {
            instances.add(new RespMediaTypesTest().withKeyType(key).withValueType(value));
         }
      }
      return instances.toArray();
   }

   @Override
   protected String parameters() {
      return String.format("[key=%s, value=%s]", keyType, valueType);
   }
}
