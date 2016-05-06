package org.infinispan.query.remote.impl;

import static org.testng.AssertJUnit.assertTrue;
import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;

@Test(groups = "functional", testName = "query.remote.impl.ProtobufMetadataCachePreserveStateAcrossRestartsTest")
public class ProtobufMetadataCachePreserveStateAcrossRestartsTest extends AbstractInfinispanTest {

   protected EmbeddedCacheManager createCacheManager(String persistentStateLocation) throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().enable().persistentLocation(persistentStateLocation);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(global, new ConfigurationBuilder());
      cacheManager.getCache();
      return cacheManager;
   }

   public void testStatePreserved() throws Exception {
      String persistentStateLocation = TestingUtil.tmpDirectory(this.getClass());
      Util.recursiveFileRemove(persistentStateLocation);

      TestingUtil.withCacheManager(new CacheManagerCallable(createCacheManager(persistentStateLocation)) {
         @Override
         public void call() throws Exception {
            Cache<String, String> protoBufMetadaCache = cm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
            protoBufMetadaCache.put("test.proto", "package X;");
         }
      });

      TestingUtil.withCacheManager(new CacheManagerCallable(createCacheManager(persistentStateLocation)) {
         @Override
         public void call() throws Exception {
            Cache<String, String> protoBufMetadaCache = cm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
            assertTrue(protoBufMetadaCache.containsKey("test.proto"));
         }
      });
   }
}
