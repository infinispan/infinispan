package org.infinispan.loaders.remote;

import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

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
      remoteCacheStoreConfig.setUseDefaultRemoteCache(true);
      assert remoteCacheStoreConfig.isUseDefaultRemoteCache();
      
      localCacheManager = TestCacheManagerFactory.createLocalCacheManager();
      Configuration configuration = localCacheManager.getDefaultConfiguration();
      configuration.setEvictionWakeUpInterval(10);
      configuration.setEvictionStrategy(EvictionStrategy.UNORDERED);
      hrServer = TestHelper.startHotRodServer(localCacheManager);
      Properties properties = new Properties();
      properties.put("hotrod-servers", "localhost:" + hrServer.getPort());
      remoteCacheStoreConfig.setHotRodClientProperties(properties);

      RemoteCacheStore remoteCacheStore = new RemoteCacheStore();
      remoteCacheStore.init(remoteCacheStoreConfig, getCache(), getMarshaller());
      remoteCacheStore.start();
      super.supportsLoadAll = false;
      return remoteCacheStore;
   }

   @AfterTest
   public void tearDown() {
      hrServer.stop();
      localCacheManager.stop();
   }

   @Override
   public void testLoadKeys() throws CacheLoaderException {
      //not applicable as relies on loadAll
   }

   @Override
   protected void purgeExpired() throws CacheLoaderException {
      localCacheManager.getCache().clear();
   }

   @Override
   public void testPreload() throws CacheLoaderException {
      //not applicable as relies on loadAll
   }

   @Override
   public void testPreloadWithMaxSize() throws CacheLoaderException {
      //not applicable as relies on loadAll
   }

   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(3000);
   }


   @Override
   public void testStoreAndRemoveAll() throws CacheLoaderException {
      //not applicable as relies on loadAll
   }

   @Override
   public void testStreamingAPI() throws IOException, ClassNotFoundException, CacheLoaderException {
      //not applicable as relies on loadAll
   }

   @Override
   public void testStreamingAPIReusingStreams() throws IOException, ClassNotFoundException, CacheLoaderException {
      //not applicable as relies on loadAll
   }
}
