package org.infinispan.query.remote.impl.xsite;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "query.remote.impl.xsite.ProtobufMetadataXSiteStateTransferTest")
public class ProtobufMetadataXSiteStateTransferTest extends AbstractTwoSitesTest {

   public ProtobufMetadataXSiteStateTransferTest() {
      cacheMode = CacheMode.DIST_SYNC;
      implicitBackupCache = true;
   }

   @Override
   protected void createSites() {
      ConfigurationBuilder lon = lonConfigurationBuilder();
      createSite(LON, initialClusterSize, globalConfigurationBuilderForSite(LON), lon);
      waitForSites(LON);
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(cacheMode);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getNycActiveConfig();
   }

   public void testMetadataStateTransfer() {
//      stopNYC();
//      waitForSites(LON);
//      TestingUtil.sleepThread(10_000);

      final String cacheName = PROTOBUF_METADATA_CACHE_NAME;
      final List<Cache<String, String>> lonMetaCaches = caches("LON", cacheName);

      final Cache<String, String> lonWriteMetaCache = lonMetaCaches.get(0);
      final String protoFileName = "testA.proto";
      final String protoFileContents = "package A;";
      lonWriteMetaCache.put(protoFileName, protoFileContents);

      final Cache<String, String> lonReadMetaCache = lonMetaCaches.get(1);
      assertEquals(lonReadMetaCache.get(protoFileName), protoFileContents);

      createNYC();
      waitForSites(LON, NYC);

      final List<Cache<String, String>> nycMetaCaches = caches("NYC", cacheName);
      final Cache<String, String> nycReadMetaCache0 = nycMetaCaches.get(0);
      assertEquals(nycReadMetaCache0.get(protoFileName), protoFileContents);

      final Cache<String, String> nycReadMetaCache1 = nycMetaCaches.get(1);
      assertEquals(nycReadMetaCache1.get(protoFileName), protoFileContents);
   }

//   private void stopNYC() {
//      final TestSite site = site(NYC);
//      site.cacheManagers.forEach(Lifecycle::stop);
//      sites.remove(site);
//   }

   private void createNYC() {
      ConfigurationBuilder nyc = getNycActiveConfig();
      nyc.sites().addBackup()
         .site(LON)
         .strategy(BackupConfiguration.BackupStrategy.SYNC)
         .sites().addInUseBackupSite(LON);
      createSite(NYC, initialClusterSize, globalConfigurationBuilderForSite(NYC), nyc);
   }

}
