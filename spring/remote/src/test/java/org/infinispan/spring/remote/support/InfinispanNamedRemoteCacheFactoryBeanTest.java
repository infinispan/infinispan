package org.infinispan.spring.remote.support;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link InfinispanNamedRemoteCacheFactoryBean}.
 * </p>
 *
 * @author Olaf Bergner
 *
 */
@Test(testName = "spring.support.remote.InfinispanNamedRemoteCacheFactoryBeanTest", groups = "functional")
public class InfinispanNamedRemoteCacheFactoryBeanTest extends SingleCacheManagerTest {

   private static final String TEST_BEAN_NAME = "test.bean.Name";

   private static final String TEST_CACHE_NAME = "test.cache.Name";

   private RemoteCacheManager remoteCacheManager;

   private HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.defineConfiguration(TEST_CACHE_NAME, cacheManager.getDefaultCacheConfiguration());
      cache = cacheManager.getCache(TEST_CACHE_NAME);
      cacheManager.defineConfiguration(TEST_BEAN_NAME, cacheManager.getDefaultCacheConfiguration());
      cache = cacheManager.getCache(TEST_BEAN_NAME);

      return cacheManager;
   }

   @BeforeClass
   public void setupRemoteCacheFactory() {
      hotrodServer = HotRodTestingUtil.startHotRodServer(cacheManager, 19733);
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
    * {@link InfinispanNamedRemoteCacheFactoryBean#afterPropertiesSet()}
    * .
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void infinispanNamedRemoteCacheFactoryBeanShouldRecognizeThatNoCacheContainerHasBeenSet()
         throws Exception {
      final InfinispanNamedRemoteCacheFactoryBean<String, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<String, Object>();
      objectUnderTest.setCacheName(TEST_CACHE_NAME);
      objectUnderTest.setBeanName(TEST_BEAN_NAME);
      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link InfinispanNamedRemoteCacheFactoryBean#setBeanName(String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanNamedRemoteCacheFactoryBeanShouldUseBeanNameAsCacheNameIfNoCacheNameHasBeenSet()
         throws Exception {
      final String beanName = TEST_BEAN_NAME;

      final InfinispanNamedRemoteCacheFactoryBean<String, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<String, Object>();
      objectUnderTest.setInfinispanRemoteCacheManager(remoteCacheManager);
      objectUnderTest.setBeanName(beanName);
      objectUnderTest.afterPropertiesSet();

      final RemoteCache<String, Object> cache = objectUnderTest.getObject();

      assertEquals("InfinispanNamedRemoteCacheFactoryBean should have used its bean name ["
                         + beanName + "] as the name of the created cache. However, it didn't.", beanName,
                   cache.getName());
   }

   /**
    * Test method for
    * {@link InfinispanNamedRemoteCacheFactoryBean#setCacheName(String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanNamedRemoteCacheFactoryBeanShouldPreferExplicitCacheNameToBeanName()
         throws Exception {
      final InfinispanNamedRemoteCacheFactoryBean<String, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<String, Object>();
      objectUnderTest.setInfinispanRemoteCacheManager(remoteCacheManager);
      objectUnderTest.setCacheName(TEST_CACHE_NAME);
      objectUnderTest.setBeanName(TEST_BEAN_NAME);
      objectUnderTest.afterPropertiesSet();

      final RemoteCache<String, Object> cache = objectUnderTest.getObject();

      assertEquals("InfinispanNamedRemoteCacheFactoryBean should have preferred its cache name ["
                         + TEST_CACHE_NAME + "] as the name of the created cache. However, it didn't.",
                   TEST_CACHE_NAME, cache.getName());
   }

   /**
    * Test method for
    * {@link InfinispanNamedRemoteCacheFactoryBean#getObjectType()}.
    *
    * @throws Exception
    */
   @Test
   public final void infinispanNamedRemoteCacheFactoryBeanShouldReportTheMostDerivedObjectType()
         throws Exception {
      final InfinispanNamedRemoteCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanRemoteCacheManager(remoteCacheManager);
      objectUnderTest.setBeanName(TEST_BEAN_NAME);
      objectUnderTest.afterPropertiesSet();

      assertEquals(
            "getObjectType() should have returned the most derived class of the actual Cache "
                  + "implementation returned from getObject(). However, it didn't.",
            objectUnderTest.getObject().getClass(), objectUnderTest.getObjectType());
   }

   /**
    * Test method for
    * {@link InfinispanNamedRemoteCacheFactoryBean#getObject()}.
    *
    * @throws Exception
    */
   @Test
   public final void infinispanNamedRemoteCacheFactoryBeanShouldProduceANonNullInfinispanCache()
         throws Exception {
      final InfinispanNamedRemoteCacheFactoryBean<String, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<String, Object>();
      objectUnderTest.setInfinispanRemoteCacheManager(remoteCacheManager);
      objectUnderTest.setCacheName(TEST_CACHE_NAME);
      objectUnderTest.setBeanName(TEST_BEAN_NAME);
      objectUnderTest.afterPropertiesSet();

      final RemoteCache<String, Object> cache = objectUnderTest.getObject();

      assertNotNull(
            "InfinispanNamedRemoteCacheFactoryBean should have produced a proper Infinispan cache. "
                  + "However, it produced a null Infinispan cache.", cache);
   }

   /**
    * Test method for
    * {@link InfinispanNamedRemoteCacheFactoryBean#isSingleton()}.
    */
   @Test
   public final void infinispanNamedRemoteCacheFactoryBeanShouldDeclareItselfToBeSingleton() {
      final InfinispanNamedRemoteCacheFactoryBean<String, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<String, Object>();

      assertTrue(
            "InfinispanNamedRemoteCacheFactoryBean should declare itself to produce a singleton. However, it didn't.",
            objectUnderTest.isSingleton());
   }
}
