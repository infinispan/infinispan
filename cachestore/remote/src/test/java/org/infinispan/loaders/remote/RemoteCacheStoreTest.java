package org.infinispan.loaders.remote;

import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "loaders.remote.RemoteCacheStoreTest", groups = "functional")
public class RemoteCacheStoreTest extends BaseCacheStoreTest {

   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @Override
   protected CacheStore createCacheStore() throws Exception {
      RemoteCacheStoreConfig remoteCacheStoreConfig = new RemoteCacheStoreConfig();
      remoteCacheStoreConfig.setPurgeSynchronously(true);
      remoteCacheStoreConfig.setUseDefaultRemoteCache(true);
      assert remoteCacheStoreConfig.isUseDefaultRemoteCache();

      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      cb.eviction().maxEntries(100).strategy(EvictionStrategy.UNORDERED)
            .expiration().wakeUpInterval(10L);

      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalConfig.globalJmxStatistics().allowDuplicateDomains(true);

      localCacheManager = TestCacheManagerFactory.createCacheManager(
            globalConfig, hotRodCacheConfiguration(cb));
      hrServer = TestHelper.startHotRodServer(localCacheManager);

      Properties properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", "localhost:" + hrServer.getPort());
      remoteCacheStoreConfig.setHotRodClientProperties(properties);

      RemoteCacheStore remoteCacheStore = new RemoteCacheStore();
      remoteCacheStore.init(remoteCacheStoreConfig, getCache(), getMarshaller());
      remoteCacheStore.start();
      return remoteCacheStore;
   }

   @Override
   @AfterMethod
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hrServer);
      TestingUtil.killCacheManagers(localCacheManager);
   }

   @Override
   protected void assertEventuallyExpires(String key) throws Exception {
      for (int i = 0; i < 10; i++) {
         if (cs.load("k") == null) break;
         Thread.sleep(1000);
      }
      assert cs.load("k") == null;
   }

   @Override
   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(3000);
   }

   @Override
   protected void purgeExpired() throws CacheLoaderException {
      localCacheManager.getCache().getAdvancedCache().getEvictionManager().processEviction();
   }

   /**
    * This is not supported, see assertion in {@link RemoteCacheStore#loadAllKeys(java.util.Set)}
    */
   @Override
   public void testLoadKeys() throws CacheLoaderException {
   }

   @Override
   public void testReplaceExpiredEntry() throws Exception {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", 100));
      // Hot Rod does not support milliseconds, so 100ms is rounded to the nearest second,
      // and so data is stored for 1 second here. Adjust waiting time accordingly.
      TestingUtil.sleepThread(1100);
      assert null == cs.load("k1");
      cs.store(TestInternalCacheEntryFactory.create("k1", "v2", 100));
      assert cs.load("k1").getValue().equals("v2");
   }
}


