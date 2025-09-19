package org.infinispan.spring.embedded.support;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.embedded.provider.BasicConfiguration;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * <p>
 * Test {@link InfinispanNamedEmbeddedCacheFactoryBean}.
 * </p>
 *
 * @author Olaf Bergner
 *
 */
@Test(testName = "spring.embedded.support.InfinispanNamedEmbeddedCacheFactoryBeanTest", groups = "unit")
@ContextConfiguration(classes = BasicConfiguration.class)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class, mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class InfinispanNamedEmbeddedCacheFactoryBeanTest extends AbstractTestNGSpringContextTests {

   private static final String CACHE_NAME_FROM_CONFIGURATION_FILE = "asyncCache";

   private static final ClassPathResource NAMED_ASYNC_CACHE_CONFIG_LOCATION = new ClassPathResource(
         "named-async-cache.xml", InfinispanNamedEmbeddedCacheFactoryBeanTest.class);

   private EmbeddedCacheManager DEFAULT_CACHE_MANAGER;

   private EmbeddedCacheManager PRECONFIGURED_DEFAULT_CACHE_MANAGER;

   @BeforeClass
   public void startCacheManagers() {
      DEFAULT_CACHE_MANAGER = TestCacheManagerFactory.createCacheManager();
      Configuration configuration = new ConfigurationBuilder().build();
      DEFAULT_CACHE_MANAGER.defineConfiguration("test.cache.Name", configuration);
      DEFAULT_CACHE_MANAGER.defineConfiguration("test.bean.Name", configuration);
      DEFAULT_CACHE_MANAGER.start();

      try (InputStream configStream = NAMED_ASYNC_CACHE_CONFIG_LOCATION.getInputStream()) {
         PRECONFIGURED_DEFAULT_CACHE_MANAGER = TestCacheManagerFactory.fromStream(configStream);
      } catch (final IOException e) {
         throw new ExceptionInInitializerError(e);
      }
   }

   @AfterClass
   public void stopCacheManagers() {
      PRECONFIGURED_DEFAULT_CACHE_MANAGER.stop();
      DEFAULT_CACHE_MANAGER.stop();
   }

   /**
    * Test method for
    * {@link InfinispanNamedEmbeddedCacheFactoryBean#afterPropertiesSet()}
    * .
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldRecognizeThatNoCacheContainerHasBeenSet()
         throws Exception {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setCacheName("test.cache.Name");
      objectUnderTest.setBeanName("test.bean.Name");
      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link InfinispanNamedEmbeddedCacheFactoryBean#setBeanName(String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldUseBeanNameAsCacheNameIfNoCacheNameHasBeenSet()
         throws Exception {
      final String beanName = "test.bean.Name";

      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setBeanName(beanName);
      objectUnderTest.afterPropertiesSet();

      final Cache<Object, Object> cache = objectUnderTest.getObject();

      assertEquals("InfinispanNamedEmbeddedCacheFactoryBean should have used its bean name ["
                         + beanName + "] as the name of the created cache. However, it didn't.", beanName,
                   cache.getName());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanNamedEmbeddedCacheFactoryBean#setCacheName(String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldPreferExplicitCacheNameToBeanName()
         throws Exception {
      final String cacheName = "test.cache.Name";
      final String beanName = "test.bean.Name";

      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(cacheName);
      objectUnderTest.setBeanName(beanName);
      objectUnderTest.afterPropertiesSet();

      final Cache<Object, Object> cache = objectUnderTest.getObject();

      assertEquals("InfinispanNamedEmbeddedCacheFactoryBean should have preferred its cache name ["
                         + cacheName + "] as the name of the created cache. However, it didn't.", cacheName,
                   cache.getName());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanNamedEmbeddedCacheFactoryBean#getObjectType()}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldReportTheMostDerivedObjectType()
         throws Exception {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setBeanName("test.bean.Name");
      objectUnderTest.afterPropertiesSet();

      assertEquals(
            "getObjectType() should have returned the most derived class of the actual Cache "
                  + "implementation returned from getObject(). However, it didn't.",
            objectUnderTest.getObject().getClass(), objectUnderTest.getObjectType());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanNamedEmbeddedCacheFactoryBean#getObject()}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldProduceANonNullInfinispanCache()
         throws Exception {
      final String cacheName = "test.cache.Name";
      final String beanName = "test.bean.Name";

      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(cacheName);
      objectUnderTest.setBeanName(beanName);
      objectUnderTest.afterPropertiesSet();

      final Cache<Object, Object> cache = objectUnderTest.getObject();

      assertNotNull(
            "InfinispanNamedEmbeddedCacheFactoryBean should have produced a proper Infinispan cache. "
                  + "However, it produced a null Infinispan cache.", cache);
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanNamedEmbeddedCacheFactoryBean#isSingleton()}
    * .
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldDeclareItselfToBeSingleton() {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();

      assertTrue(
            "InfinispanNamedEmbeddedCacheFactoryBean should declare itself to produce a singleton. However, it didn't.",
            objectUnderTest.isSingleton());
   }

   /**
    * Test method for
    * {@link InfinispanNamedEmbeddedCacheFactoryBean#destroy()}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldStopTheCreatedInfinispanCacheWhenItIsDestroyed()
         throws Exception {
      final String cacheName = "test.cache.Name";
      final String beanName = "test.bean.Name";

      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(cacheName);
      objectUnderTest.setBeanName(beanName);
      objectUnderTest.afterPropertiesSet();

      final Cache<Object, Object> cache = objectUnderTest.getObject();
      objectUnderTest.destroy();

      assertEquals(
            "InfinispanNamedEmbeddedCacheFactoryBean should have stopped the created Infinispan cache when being destroyed. "
                  + "However, the created Infinispan is not yet terminated.",
            ComponentStatus.TERMINATED, cache.getStatus());
   }

   /**
    * Test method for
    * {@link InfinispanNamedEmbeddedCacheFactoryBean#afterPropertiesSet()}
    * .
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldRejectConfigurationTemplateModeNONEIfCacheConfigurationAlreadyExistsInConfigurationFile()
         throws Exception {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(PRECONFIGURED_DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(CACHE_NAME_FROM_CONFIGURATION_FILE);
      objectUnderTest.setBeanName(CACHE_NAME_FROM_CONFIGURATION_FILE);
      objectUnderTest.setConfigurationTemplateMode("NONE");
      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link InfinispanNamedEmbeddedCacheFactoryBean#afterPropertiesSet()}
    * .
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldRejectConfigurationTemplateModeDEFAULTIfCacheConfigurationAlreadyExistsInConfigurationFile()
         throws Exception {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(PRECONFIGURED_DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(CACHE_NAME_FROM_CONFIGURATION_FILE);
      objectUnderTest.setBeanName(CACHE_NAME_FROM_CONFIGURATION_FILE);
      objectUnderTest.setConfigurationTemplateMode("DEFAULT");
      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Negative test case for {@link InfinispanNamedEmbeddedCacheFactoryBean#addCustomConfiguration(ConfigurationBuilder)}
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void infinispanNamedEmbeddedCacheFactoryShouldRejectConfigurationTemplateModeCUSTOM() throws
                                                                                                      Exception {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new
            InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(CACHE_NAME_FROM_CONFIGURATION_FILE);
      objectUnderTest.setConfigurationTemplateMode("CUSTOM");
      objectUnderTest.afterPropertiesSet();
   }

   @Test
   public final void infinispanNamedEmbeddedCacheFactoryShouldAcceptConfigurationTemplateModeCUSTOM() throws
                                                                                                      Exception {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new
            InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(CACHE_NAME_FROM_CONFIGURATION_FILE);
      objectUnderTest.setConfigurationTemplateMode("CUSTOM");
      ConfigurationBuilder custom = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      objectUnderTest.addCustomConfiguration(custom);
      objectUnderTest.afterPropertiesSet();
   }
}
