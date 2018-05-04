package org.infinispan.query.remote.impl.xsite;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "query.remote.impl.xsite.ProtobufMetadataXSiteTest")
public class ProtobufMetadataXSiteTest extends AbstractTwoSitesTest {

   public ProtobufMetadataXSiteTest() {
      cacheMode = CacheMode.DIST_SYNC;
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(cacheMode);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getNycActiveConfig();
   }

   public void testMetadataReplicatedXSite() {
      final String cacheName = PROTOBUF_METADATA_CACHE_NAME;
      final List<Cache<String, String>> nycMetaCaches = caches("NYC", cacheName);

      final Cache<String, String> nycWriteMetaCache = nycMetaCaches.get(0);
      final String protoFileName = "testA.proto";
      final String protoFileContents = "package A;";
      nycWriteMetaCache.put(protoFileName, protoFileContents);

      final Cache<String, String> nyReadMetaCache = nycMetaCaches.get(1);
      assertEquals(nyReadMetaCache.get(protoFileName), protoFileContents);

      final List<Cache<String, String>> lonMetaCaches = caches("LON", cacheName);
      final Cache<String, String> lonReadMetaCache0 = lonMetaCaches.get(0);
      assertEquals(lonReadMetaCache0.get(protoFileName), protoFileContents);

      final Cache<String, String> lonReadMetaCache1 = lonMetaCaches.get(1);
      assertEquals(lonReadMetaCache1.get(protoFileName), protoFileContents);
   }

}
