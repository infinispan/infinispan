package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.BaseTombstonePersistenceTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.PersistenceMockUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests tombstone stored in {@link RemoteStore}.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "persistence.remote.RemoteStoreTombstoneTest")
public class RemoteStoreTombstoneTest extends BaseTombstonePersistenceTest {

   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @BeforeMethod(alwaysRun = true)
   @Override
   public void setUp() throws Exception {
      localCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      hrServer = HotRodClientTestingUtil.startHotRodServer(localCacheManager);
      super.setUp();
   }

   @AfterMethod(alwaysRun = true)
   @Override
   public void tearDown() throws PersistenceException {
      super.tearDown();
      HotRodClientTestingUtil.killServers(hrServer);
      hrServer = null;
      TestingUtil.killCacheManagers(localCacheManager);
      localCacheManager = null;
   }

   @Override
   protected WaitNonBlockingStore<String, String> getStore() {
      return wrapAndStart(new RemoteStore<>(), createContext());
   }

   @Override
   protected boolean keysStreamContainsTombstones() {
      return true;
   }

   private InitializationContext createContext() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL).hash().numSegments(numSegments());
      builder.persistence().addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName("")
            .segmented(false)
            .addServer()
            .host(hrServer.getHost())
            .port(hrServer.getPort());
      return PersistenceMockUtil.createContext(getClass(), builder.build(), getMarshaller());
   }
}
