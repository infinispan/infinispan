package org.infinispan.persistence.remote;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test for getWithMetadata backed with a remote store
 */
@Test(testName = "persistence.remote.GetWithMetadataTest", groups = "functional")
public class GetWithMetadataTest extends AbstractInfinispanTest {

   public static final String CACHE_NAME = "testCache";

   private <K, V> RemoteCache<K, V> getRemoteCache(HotRodServer hotRodServer) {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(
            HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotRodServer)
                  .marshaller(GenericJBossMarshaller.class)
                  .build()
      );
      return remoteCacheManager.getCache(CACHE_NAME);
   }

   protected ConfigurationBuilder getTargetCacheConfiguration(int sourcePort) {
      ConfigurationBuilder cb = hotRodCacheConfiguration(MediaType.APPLICATION_JBOSS_MARSHALLING);

      cb.persistence()
            .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName(CACHE_NAME)
            // Store cannot be segmented as the remote cache is LOCAL and it doesn't report its segments?
            .segmented(false)
            .addServer()
            .host("localhost")
            .port(sourcePort)
            .shared(true)
            .addProperty(RemoteStore.MIGRATION, "true");
      return cb;
   }

   public void testGetWithMetadata() {
      EmbeddedCacheManager sourceCacheManager = null;
      EmbeddedCacheManager targetCacheManager = null;
      HotRodServer sourceServer = null;
      HotRodServer targetServer = null;
      RemoteCache<String, String> sourceRemoteCache = null;
      RemoteCache<String, String> targetRemoteCache = null;
      try {
         // Create source hotrod server
         sourceCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
         sourceCacheManager.defineConfiguration(CACHE_NAME, hotRodCacheConfiguration(MediaType.APPLICATION_JBOSS_MARSHALLING).build());
         sourceServer = HotRodClientTestingUtil.startHotRodServer(sourceCacheManager);

         // Put some entries
         sourceRemoteCache = getRemoteCache(sourceServer);
         sourceRemoteCache.put("key", "value");
         sourceRemoteCache.put("key2", "value2", 48, TimeUnit.HOURS, 1, TimeUnit.DAYS);
         sourceRemoteCache.put("key3", "value2");

         MetadataValue<String> key2Metadata = sourceRemoteCache.getWithMetadata("key2");
         long k2Created = key2Metadata.getCreated();

         // Create target hotrod server, with a remote cache loader pointing to the source one
         targetCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration(MediaType.APPLICATION_JBOSS_MARSHALLING));
         targetServer = HotRodClientTestingUtil.startHotRodServer(targetCacheManager);
         ConfigurationBuilder targetCacheConfiguration = getTargetCacheConfiguration(sourceServer.getPort());
         targetCacheManager.defineConfiguration(CACHE_NAME, targetCacheConfiguration.build());

         // Try a get with metadata from the target server
         targetRemoteCache = getRemoteCache(targetServer);
         MetadataValue<String> metadataEntry = targetRemoteCache.getWithMetadata("key");
         assertNotNull(metadataEntry);

         MetadataValue<String> otherMetadataEntry = targetRemoteCache.getWithMetadata("key2");
         assertNotNull(otherMetadataEntry);
         assertEquals(48 * 3600, otherMetadataEntry.getLifespan());
         assertEquals(24 * 3600, otherMetadataEntry.getMaxIdle());
         assertEquals(otherMetadataEntry.getCreated(), k2Created);
         assertTrue(otherMetadataEntry.getLastUsed() > 0);

      } finally {
         killRemoteCacheManager(targetRemoteCache != null ? targetRemoteCache.getRemoteCacheContainer() : null);
         killRemoteCacheManager(sourceRemoteCache != null ? sourceRemoteCache.getRemoteCacheContainer() : null);
         killCacheManagers(targetCacheManager, sourceCacheManager);
         killServers(targetServer, sourceServer);
      }

   }

}
