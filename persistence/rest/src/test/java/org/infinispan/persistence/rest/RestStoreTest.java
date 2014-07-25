package org.infinispan.persistence.rest;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.rest.configuration.RestStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.rest.EmbeddedRestServer;
import org.infinispan.rest.RestTestingUtil;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.TimeService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * @author Tristan Tarrant
 * @since 6.0
 */
@Test(testName = "persistence.rest.RestStoreTest", groups = "functional")
public class RestStoreTest extends BaseStoreTest {

   private static final String REMOTE_CACHE = "remote-cache";
   private EmbeddedCacheManager localCacheManager;
   private EmbeddedRestServer restServer;

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder localBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      localBuilder.eviction().maxEntries(100).strategy(EvictionStrategy.UNORDERED).expiration().wakeUpInterval(10L);

      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalConfig.globalJmxStatistics().allowDuplicateDomains(true);

      localCacheManager = TestCacheManagerFactory.createCacheManager(globalConfig, localBuilder);
      localCacheManager.getCache(REMOTE_CACHE);
      GlobalComponentRegistry gcr = localCacheManager.getGlobalComponentRegistry();
      gcr.registerComponent(timeService, TimeService.class);
      gcr.rewire();
      localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache().getComponentRegistry().rewire();
      restServer = RestTestingUtil.startRestServer(localCacheManager);

      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      RestStoreConfigurationBuilder storeConfigurationBuilder = builder.persistence()
            .addStore(RestStoreConfigurationBuilder.class);
      storeConfigurationBuilder.host(restServer.getHost()).port(restServer.getPort()).path("/rest/" + REMOTE_CACHE);
      storeConfigurationBuilder.connectionPool().maxTotalConnections(10).maxConnectionsPerHost(10);
      storeConfigurationBuilder.validate();
      RestStore restStore = new RestStore();
      restStore.init(createContext(builder.build()));
      return restStore;
   }

   @Override
   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      if (restServer != null) {
         RestTestingUtil.killServers(restServer);
      }
      if (localCacheManager != null) {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }

   @Override
   public void testReplaceExpiredEntry() throws Exception {
      InternalCacheEntry ice = internalCacheEntry("k1", "v1", 100);
      cl.write(marshalledEntry(ice));
      // Hot Rod does not support milliseconds, so 100ms is rounded to the nearest second,
      // and so data is stored for 1 second here. Adjust waiting time accordingly.
      timeService.advance(1101);
      assertNull(cl.load("k1"));
      InternalCacheEntry ice2 = internalCacheEntry("k1", "v2", 100);
      cl.write(marshalledEntry(ice2));
      assertEquals("v2", cl.load("k1").getValue());
   }

}
