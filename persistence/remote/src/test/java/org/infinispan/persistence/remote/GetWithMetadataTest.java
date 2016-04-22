package org.infinispan.persistence.remote;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Test for getWithMetadata backed with a remote store
 */
@Test(testName = "persistence.remote.GetWithMetadataTest", groups = "functional")
public class GetWithMetadataTest extends AbstractInfinispanTest {

   public static final String CACHE_NAME = "testCache";

   protected PersistenceConfigurationBuilder createCacheStoreConfig(String cacheName, int port,
                                                                    PersistenceConfigurationBuilder persistence) {
      persistence.addStore(RemoteStoreConfigurationBuilder.class)
              .remoteCacheName(cacheName)
              .hotRodWrapping(true)
              .addServer()
              .host("localhost")
              .port(port);
      return persistence;
   }

   private <K, V> RemoteCache<K, V> getRemoteCache(HotRodServer hotRodServer) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
              new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      RemoteCacheManager remoteCacheManager =
              new RemoteCacheManager(clientBuilder.addServer().host("localhost").port(hotRodServer.getPort()).build());
      return remoteCacheManager.getCache(CACHE_NAME);
   }

   protected ConfigurationBuilder getTargetCacheConfiguration(int sourcePort) {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(CACHE_NAME, sourcePort, cb.persistence());
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
         sourceCacheManager.defineConfiguration(CACHE_NAME, hotRodCacheConfiguration().build());
         sourceServer = HotRodClientTestingUtil.startHotRodServer(sourceCacheManager);

         // Put some entries
         sourceRemoteCache = getRemoteCache(sourceServer);
         sourceRemoteCache.put("key", "value");
         sourceRemoteCache.put("key2", "value2", 24, TimeUnit.HOURS, 1, TimeUnit.DAYS);
         sourceRemoteCache.put("key3", "value2");

         MetadataValue<String> key2Metadata = sourceRemoteCache.getWithMetadata("key2");
         long k2Created = key2Metadata.getCreated();

         // Create target hotrod server, with a remote cache loader pointing to the source one
         targetCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
         targetServer = HotRodClientTestingUtil.startHotRodServer(targetCacheManager);
         ConfigurationBuilder targetCacheConfiguration = getTargetCacheConfiguration(sourceServer.getPort());
         targetCacheManager.defineConfiguration(CACHE_NAME, targetCacheConfiguration.build());

         // Try a get with metadata from the target server
         targetRemoteCache = getRemoteCache(targetServer);
         MetadataValue<String> metadataEntry = targetRemoteCache.getWithMetadata("key");
         assertNotNull(metadataEntry);

         MetadataValue<String> otherMetadataEntry = targetRemoteCache.getWithMetadata("key2");
         assertNotNull(otherMetadataEntry);
         assertEquals(otherMetadataEntry.getLifespan(), 24 * 3600);
         assertEquals(otherMetadataEntry.getMaxIdle(), 24 * 3600);
         assertEquals(otherMetadataEntry.getCreated(), k2Created);
         assertTrue(otherMetadataEntry.getLastUsed() > 0);

      } finally {
         killRemoteCacheManager(targetRemoteCache != null ? targetRemoteCache.getRemoteCacheManager() : null);
         killRemoteCacheManager(sourceRemoteCache != null ? sourceRemoteCache.getRemoteCacheManager() : null);
         killCacheManagers(targetCacheManager, sourceCacheManager);
         killServers(targetServer, sourceServer);
      }

   }

}
