package org.infinispan.persistence.remote;

import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.internalMetadata;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;


/**
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(testName = "persistence.remote.RemoteStoreRawValuesTest", groups = "functional")
public class RemoteStoreRawValuesTest extends BaseStoreTest {

   private static final String REMOTE_CACHE = "remote-cache";
   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @Override
   @AfterMethod
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hrServer);
      TestingUtil.killCacheManagers(localCacheManager);
   }

   @Override
   public void testLoadAll() throws PersistenceException {
   }

   @Override
   public void testReplaceExpiredEntry() throws Exception {
      cl.write(new MarshalledEntryImpl("k1", "v1", internalMetadata(100l, null), getMarshaller()));
      // Hot Rod does not support milliseconds, so 100ms is rounded to the nearest second,
      // and so data is stored for 1 second here. Adjust waiting time accordingly.
      TestingUtil.sleepThread(1100);
      assertNull(cl.load("k1"));
      long start = System.currentTimeMillis();
      cl.write(new MarshalledEntryImpl("k1", "v2", internalMetadata(100l, null), getMarshaller()));
      assertTrue(cl.load("k1").getValue().equals("v2") || TestingUtil.moreThanDurationElapsed(start, 100));
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder localBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      localBuilder.eviction().maxEntries(100).strategy(EvictionStrategy.UNORDERED)
            .expiration().wakeUpInterval(10L);

      localCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration(localBuilder));

      localCacheManager.getCache(REMOTE_CACHE);
      hrServer = TestHelper.startHotRodServer(localCacheManager);

      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      RemoteStoreConfigurationBuilder storeConfigurationBuilder = builder
            .persistence()
            .addStore(RemoteStoreConfigurationBuilder.class)
            .rawValues(true)
            .remoteCacheName(REMOTE_CACHE);
      storeConfigurationBuilder
            .addServer()
            .host(hrServer.getHost())
            .port(hrServer.getPort());

      RemoteStore remoteStore = new RemoteStore();
      remoteStore.init(createContext(builder.build()));
      return remoteStore;
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return localCacheManager.getCache("dummy").getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   @Override
   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(3000);
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }
}

