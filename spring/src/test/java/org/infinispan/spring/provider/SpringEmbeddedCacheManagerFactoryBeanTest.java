package org.infinispan.spring.provider;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.TransactionMode;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringEmbeddedCacheManagerFactoryBean}.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 * 
 */
@Test(testName = "spring.provider.SpringEmbeddedCacheManagerFactoryBeanTest", groups = "unit")
public class SpringEmbeddedCacheManagerFactoryBeanTest {

   private static final String CACHE_NAME_FROM_CONFIGURATION_FILE = "asyncCache";

   private static final String NAMED_ASYNC_CACHE_CONFIG_LOCATION = "named-async-cache.xml";

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setConfigurationFileLocation(org.springframework.core.io.Resource)}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldCreateACacheManagerEvenIfNoDefaultConfigurationLocationHasBeenSet()
            throws Exception {
      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertNotNull(
            "getObject() should have returned a valid SpringEmbeddedCacheManager, even if no defaulConfigurationLocation "
                  + "has been specified. However, it returned null.",
            springEmbeddedCacheManager);
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setConfigurationFileLocation(org.springframework.core.io.Resource)}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldCreateACustomizedCacheManagerIfGivenADefaultConfigurationLocation()
            throws Exception {
      final Resource infinispanConfig = new ClassPathResource(NAMED_ASYNC_CACHE_CONFIG_LOCATION,
               getClass());

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setConfigurationFileLocation(infinispanConfig);
      objectUnderTest.afterPropertiesSet();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();
      assertNotNull(
               "getObject() should have returned a valid SpringEmbeddedCacheManager, configured using the configuration file "
                        + "set on SpringEmbeddedCacheManagerFactoryBean. However, it returned null.",
               springEmbeddedCacheManager);
      final SpringCache cacheDefinedInCustomConfiguration = springEmbeddedCacheManager
               .getCache(CACHE_NAME_FROM_CONFIGURATION_FILE);
      final org.infinispan.configuration.cache.Configuration configuration = ((Cache)cacheDefinedInCustomConfiguration.getNativeCache())
               .getCacheConfiguration();
      assertEquals(
               "The cache named ["
                        + CACHE_NAME_FROM_CONFIGURATION_FILE
                        + "] is configured to have asynchonous replication cache mode. Yet, the cache returned from getCache("
                        + CACHE_NAME_FROM_CONFIGURATION_FILE
                        + ") has a different cache mode. Obviously, SpringEmbeddedCacheManagerFactoryBean did not use "
                        + "the configuration file when instantiating SpringEmbeddedCacheManager.",
               org.infinispan.configuration.cache.CacheMode.REPL_ASYNC,
               configuration.clustering().cacheMode());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#getObjectType()}.
    * 
    * @throws Exception
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldReportTheCorrectObjectType()
            throws Exception {
      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "getObjectType() should return the most derived class of the actual SpringEmbeddedCacheManager "
                        + "implementation returned from getObject(). However, it didn't.",
               springEmbeddedCacheManager.getClass(), objectUnderTest.getObjectType());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#isSingleton()}.
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldDeclareItselfToOnlyProduceSingletons() {
      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();

      assertTrue("isSingleton() should always return true. However, it returned false",
            objectUnderTest.isSingleton());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#destroy()}.
    * 
    * @throws Exception
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldStopTheCreateEmbeddedCacheManagerWhenBeingDestroyed()
            throws Exception {
      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();
      springEmbeddedCacheManager.getCache("default"); // Implicitly starts
                                                      // SpringEmbeddedCacheManager
      objectUnderTest.destroy();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should stop the created SpringEmbeddedCacheManager when being destroyed. "
                        + "However, the created SpringEmbeddedCacheManager is still not terminated.",
               ComponentStatus.TERMINATED, springEmbeddedCacheManager.getNativeCacheManager()
                        .getStatus());
      springEmbeddedCacheManager.stop();
   }

   // Testing the addBuilder() methods
   @Test
   public final void testAddConfigurations() throws Exception {
      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();

      // Allow duplicate domains. A good little configuration modification to make. If this isn't enabled,
      // JMXDomainConflicts occur which break the testsuite. This way we can also have a non-default configuration to
      // check.
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.globalJmxStatistics().allowDuplicateDomains(true);

      // Now prepare a cache configuration.
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      // Now add them to the object that we are testing.
      objectUnderTest.addCustomGlobalConfiguration(gcb);
      objectUnderTest.addCustomCacheConfiguration(builder);
      objectUnderTest.afterPropertiesSet();

      // Get the cache manager and make assertions.
      final EmbeddedCacheManager infinispanEmbeddedCacheManager = objectUnderTest.getObject().getNativeCacheManager();
      assertEquals(infinispanEmbeddedCacheManager.getCacheManagerConfiguration().globalJmxStatistics()
            .allowDuplicateDomains(), gcb.build().globalJmxStatistics().allowDuplicateDomains());
      assertEquals(infinispanEmbeddedCacheManager.getDefaultCacheConfiguration().transaction().transactionMode().isTransactional(),
            builder.build().transaction().transactionMode().isTransactional());
   }
}
