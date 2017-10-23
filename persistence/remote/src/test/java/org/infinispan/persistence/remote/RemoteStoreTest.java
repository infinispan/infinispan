package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.TimeService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "persistence.remote.RemoteStoreTest", groups = "functional")
public class RemoteStoreTest extends BaseStoreTest {

   private static final String REMOTE_CACHE = "remote-cache";
   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder localBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      localBuilder.memory().evictionType(EvictionType.COUNT).size(WRITE_DELETE_BATCH_MAX_ENTRIES).expiration().wakeUpInterval(10L);

      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalConfig.globalJmxStatistics().defaultCacheName(REMOTE_CACHE);

      localCacheManager = TestCacheManagerFactory.createCacheManager(
            globalConfig, hotRodCacheConfiguration(localBuilder));
      localCacheManager.getCache(REMOTE_CACHE);
      GlobalComponentRegistry gcr = localCacheManager.getGlobalComponentRegistry();
      gcr.registerComponent(timeService, TimeService.class);
      gcr.rewire();
      localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache().getComponentRegistry().rewire();
      hrServer = HotRodClientTestingUtil.startHotRodServer(localCacheManager);
      // In case if the server has to unmarshall the value, make sure to use the same marshaller
      hrServer.setMarshaller(getMarshaller());

      ConfigurationBuilder builder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false);
      RemoteStoreConfigurationBuilder storeConfigurationBuilder = builder
            .persistence()
               .addStore(RemoteStoreConfigurationBuilder.class)
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
      return localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   @Override
   @AfterMethod
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hrServer);
      TestingUtil.killCacheManagers(localCacheManager);
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }

   @Override
   public void testReplaceExpiredEntry() throws Exception {
      cl.write(marshalledEntry(internalCacheEntry("k1", "v1", 100l)));
      // Hot Rod does not support milliseconds, so 100ms is rounded to the nearest second,
      // and so data is stored for 1 second here. Adjust waiting time accordingly.
      timeService.advance(1101);
      assertNull(cl.load("k1"));
      long start = System.currentTimeMillis();
      cl.write(marshalledEntry(internalCacheEntry("k1", "v2", 100l)));
      assertTrue(cl.load("k1").getValue().equals("v2") || TestingUtil.moreThanDurationElapsed(start, 100));
   }
}
