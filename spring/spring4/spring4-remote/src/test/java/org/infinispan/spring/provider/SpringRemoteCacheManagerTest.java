package org.infinispan.spring.provider;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.cache.Cache;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringRemoteCacheManager}.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 *
 */
@Test(testName = "spring.provider.SpringRemoteCacheManagerTest", groups = {"functional", "smoke"})
public class SpringRemoteCacheManagerTest extends SingleCacheManagerTest {

   private static final String TEST_CACHE_NAME = "spring.remote.cache.manager.Test";

   private RemoteCacheManager remoteCacheManager;

   private HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.defineConfiguration(TEST_CACHE_NAME, cacheManager.getDefaultCacheConfiguration());
      cache = cacheManager.getCache(TEST_CACHE_NAME);

      return cacheManager;
   }

   @BeforeClass
   public void setupRemoteCacheFactory() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      hotrodServer = HotRodTestingUtil.startHotRodServer(cacheManager, ServerTestingUtil.findFreePort(), serverBuilder);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(builder.build());
   }

   @AfterClass
   public void destroyRemoteCacheFactory() {
      remoteCacheManager.stop();
      hotrodServer.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManager#SpringRemoteCacheManager(org.infinispan.client.hotrod.RemoteCacheManager)}
    * .
    */
   @Test(expectedExceptions = IllegalArgumentException.class)
   public final void springRemoteCacheManagerConstructorShouldRejectNullRemoteCacheManager() {
      new SpringRemoteCacheManager(null);
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManager#getCache(java.lang.String)}.
    */
   @Test
   public final void springRemoteCacheManagerShouldProperlyCreateCache() {
      final SpringRemoteCacheManager objectUnderTest = new SpringRemoteCacheManager(
            remoteCacheManager);

      final Cache defaultCache = objectUnderTest.getCache(TEST_CACHE_NAME);

      assertNotNull("getCache(" + TEST_CACHE_NAME
                          + ") should have returned a default cache. However, it returned null.", defaultCache);
      assertEquals("getCache(" + TEST_CACHE_NAME + ") should have returned a cache name \""
                         + TEST_CACHE_NAME + "\". However, the returned cache has a different name.",
                   TEST_CACHE_NAME, defaultCache.getName());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManager#getCacheNames()}.
    */
   @Test
   public final void getCacheNamesShouldReturnAllCachesDefinedInConfigurationFile() {
      final SpringRemoteCacheManager objectUnderTest = new SpringRemoteCacheManager(remoteCacheManager);
      final Collection<String> cacheNames = objectUnderTest.getCacheNames();
      assertTrue("SpringRemoteCacheManager should load all named caches found in the "
            + "native cache manager. However, it does not know about the cache named "
            + TEST_CACHE_NAME
            + " defined in said cache manager.",
            cacheNames.contains(TEST_CACHE_NAME));
   }

   /**
    * Test method for {@link org.infinispan.spring.provider.SpringRemoteCacheManager#start()}.
    *
    * @throws IOException
    */
   @Test
   public final void startShouldStartTheNativeRemoteCacheManager() throws IOException {
      final RemoteCacheManager nativeCacheManager = new RemoteCacheManager(true);
      final SpringRemoteCacheManager objectUnderTest = new SpringRemoteCacheManager(
            nativeCacheManager);

      objectUnderTest.start();

      assertTrue("Calling start() on SpringRemoteCacheManager should start the enclosed "
                       + "Infinispan RemoteCacheManager. However, it is still not running.",
                 nativeCacheManager.isStarted());
      nativeCacheManager.stop();
   }

   /**
    * Test method for {@link org.infinispan.spring.provider.SpringRemoteCacheManager#stop()}.
    *
    * @throws IOException
    */
   @Test
   public final void stopShouldStopTheNativeRemoteCacheManager() throws IOException {
      final RemoteCacheManager nativeCacheManager = new RemoteCacheManager(true);
      final SpringRemoteCacheManager objectUnderTest = new SpringRemoteCacheManager(
            nativeCacheManager);

      objectUnderTest.stop();

      assertFalse("Calling stop() on SpringRemoteCacheManager should stop the enclosed "
                        + "Infinispan RemoteCacheManager. However, it is still running.",
                  nativeCacheManager.isStarted());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManager#getNativeCacheManager()}.
    *
    * @throws IOException
    */
   @Test
   public final void getNativeCacheShouldReturnTheRemoteCacheManagerSuppliedAtConstructionTime()
         throws IOException {
      final RemoteCacheManager nativeCacheManager = new RemoteCacheManager(true);
      final SpringRemoteCacheManager objectUnderTest = new SpringRemoteCacheManager(
            nativeCacheManager);

      final RemoteCacheManager nativeCacheManagerReturned = objectUnderTest.getNativeCacheManager();

      assertSame(
            "getNativeCacheManager() should have returned the RemoteCacheManager supplied at construction time. However, it retuned a different one.",
            nativeCacheManager, nativeCacheManagerReturned);
      nativeCacheManager.stop();
   }
}
