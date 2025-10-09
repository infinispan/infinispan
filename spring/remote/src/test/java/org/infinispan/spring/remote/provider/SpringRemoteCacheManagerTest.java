package org.infinispan.spring.remote.provider;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.cache.Cache;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringRemoteCacheManager}.
 * </p>
 *
 * @author Olaf Bergner
 * @author Marius Bogoevici
 *
 */
@Test(testName = "spring.provider.SpringRemoteCacheManagerTest", groups = {"functional", "smoke"})
public class SpringRemoteCacheManagerTest extends SingleCacheManagerTest {

   private static final String TEST_CACHE_NAME = "spring.remote.cache.manager.Test";
   private static final String OTHER_TEST_CACHE_NAME = "spring.remote.cache.manager.OtherTest";

   private RemoteCacheManager remoteCacheManager;

   private HotRodServer hotrodServer;
   private SpringRemoteCacheManager objectUnderTest;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.defineConfiguration(OTHER_TEST_CACHE_NAME, cacheManager.getDefaultCacheConfiguration());
      return cacheManager;
   }

   @BeforeMethod
   public void createCache(){
      if(objectUnderTest != null) {
         objectUnderTest.start();
      }
      cacheManager.administration().removeCache(TEST_CACHE_NAME);
      cacheManager.undefineConfiguration(TEST_CACHE_NAME);
      cacheManager.defineConfiguration(TEST_CACHE_NAME, cacheManager.getDefaultCacheConfiguration());
      cache = cacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).getOrCreateCache(TEST_CACHE_NAME, TEST_CACHE_NAME);
      objectUnderTest = new SpringRemoteCacheManager(remoteCacheManager);
   }

   @AfterMethod(alwaysRun = true)
   public void afterMethod() {
      if (objectUnderTest != null) {
         objectUnderTest.stop();
      }
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

   @AfterClass(alwaysRun = true)
   public void destroyRemoteCacheFactory() {
      remoteCacheManager.stop();
      hotrodServer.stop();
   }


   /**
    * Test method for
    * {@link SpringRemoteCacheManager#SpringRemoteCacheManager(RemoteCacheManager)}
    * .
    */
   @Test(expectedExceptions = IllegalArgumentException.class)
   public final void springRemoteCacheManagerConstructorShouldRejectNullRemoteCacheManager() {
      new SpringRemoteCacheManager(null);
   }

   /**
    * Test method for
    * {@link SpringRemoteCacheManager#getCache(String)}.
    */
   @Test
   public final void springRemoteCacheManagerShouldProperlyCreateCache() {
      final Cache defaultCache = objectUnderTest.getCache(TEST_CACHE_NAME);

      assertNotNull("getCache(" + TEST_CACHE_NAME
                          + ") should have returned a default cache. However, it returned null.", defaultCache);
      assertEquals("getCache(" + TEST_CACHE_NAME + ") should have returned a cache name \""
                         + TEST_CACHE_NAME + "\". However, the returned cache has a different name.",
                   TEST_CACHE_NAME, defaultCache.getName());
   }

   /**
    * Test method for
    * {@link SpringRemoteCacheManager#getCacheNames()}.
    */
   @Test
   public final void getCacheNamesShouldReturnAllCachesDefinedInConfigurationFile() {
      final Collection<String> cacheNames = objectUnderTest.getCacheNames();
      assertTrue("SpringRemoteCacheManager should load all named caches found in the "
            + "native cache manager. However, it does not know about the cache named "
            + TEST_CACHE_NAME
            + " defined in said cache manager.",
            cacheNames.contains(TEST_CACHE_NAME));
   }

   /**
    * Test method for {@link SpringRemoteCacheManager#start()}.
    *
    * @throws IOException
    */
   @Test
   public final void startShouldStartTheNativeRemoteCacheManager() throws IOException {
      objectUnderTest.start();

      assertTrue("Calling start() on SpringRemoteCacheManager should start the enclosed "
                       + "Infinispan RemoteCacheManager. However, it is still not running.",
                 remoteCacheManager.isStarted());
   }

   /**
    * Test method for {@link SpringRemoteCacheManager#stop()}.
    *
    * @throws IOException
    */
   @Test
   public final void stopShouldStopTheNativeRemoteCacheManager() throws IOException {
      objectUnderTest.stop();

      assertFalse("Calling stop() on SpringRemoteCacheManager should stop the enclosed "
                        + "Infinispan RemoteCacheManager. However, it is still running.",
                  remoteCacheManager.isStarted());
   }

   /**
    * Test method for
    * {@link SpringRemoteCacheManager#getNativeCacheManager()}.
    *
    */
   @Test
   public final void getNativeCacheShouldReturnTheRemoteCacheManagerSuppliedAtConstructionTime() {

      final RemoteCacheManager nativeCacheManagerReturned = objectUnderTest.getNativeCacheManager();

      assertSame(
            "getNativeCacheManager() should have returned the RemoteCacheManager supplied at construction time. However, it retuned a different one.",
            remoteCacheManager, nativeCacheManagerReturned);
   }

   @Test
   public final void getCacheShouldReturnSameInstanceForSameName() {
      // When
      final SpringCache firstObtainedSpringCache = objectUnderTest.getCache(TEST_CACHE_NAME);
      final SpringCache secondObtainedSpringCache = objectUnderTest.getCache(TEST_CACHE_NAME);

      // Then
      assertSame(
              "getCache() should have returned the same SpringCache instance for the same name",
              firstObtainedSpringCache, secondObtainedSpringCache);
   }

   @Test
   public final void getCacheShouldReturnDifferentInstancesForDifferentNames() {
      // When
      final SpringCache firstObtainedSpringCache = objectUnderTest.getCache(TEST_CACHE_NAME);
      final SpringCache secondObtainedSpringCache = objectUnderTest.getCache(OTHER_TEST_CACHE_NAME);

      // Then
      assertNotSame(
              "getCache() should have returned different SpringCache instances for different names",
              firstObtainedSpringCache, secondObtainedSpringCache);
   }

   @Test
   public final void getCacheReturnsDifferentInstanceForSameNameAfterLifecycleStop() {
      final SpringCache firstObtainedSpringCache = objectUnderTest.getCache(TEST_CACHE_NAME);

      // When
      objectUnderTest.stop();
      final SpringCache secondObtainedSpringCache = objectUnderTest.getCache(TEST_CACHE_NAME);

      // Then
      assertNotSame(
              "getCache() should have returned different SpringCache instances for the sam name after a Lifecycle#stop()",
              firstObtainedSpringCache, secondObtainedSpringCache);
   }

   @Test
   public final void getCacheShouldReturnNullItWasChangedByRemoteCacheManager() {
      // When
      objectUnderTest.getCache(TEST_CACHE_NAME);
      remoteCacheManager.administration().removeCache(TEST_CACHE_NAME);

      // Then
      assertNull(objectUnderTest.getCache(TEST_CACHE_NAME));
   }

}
