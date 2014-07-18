package org.infinispan.spring.support.embedded;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.transaction.TransactionMode;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.*;

/**
 * <p>
 * Test {@link SpringEmbeddedCacheManagerFactoryBean}.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 *
 */
@Test(testName = "spring.support.embedded.InfinispanEmbeddedCacheManagerFactoryBeanTest", groups = "unit")
public class InfinispanEmbeddedCacheManagerFactoryBeanTest {

   private static final String CACHE_NAME_FROM_CONFIGURATION_FILE = "asyncCache";

   private static final String NAMED_ASYNC_CACHE_CONFIG_LOCATION = "named-async-cache.xml";

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanEmbeddedCacheManagerFactoryBean#setConfigurationFileLocation(org.springframework.core.io.Resource)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanEmbeddedCacheManagerFactoryBeanShouldCreateACacheManagerEvenIfNoDefaultConfigurationLocationHasBeenSet()
         throws Exception {
      final InfinispanEmbeddedCacheManagerFactoryBean objectUnderTest = new TestInfinispanEmbeddedCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      withCacheManager(new CacheManagerCallable(objectUnderTest.getObject()) {
         @Override
         public void call() {
            assertNotNull(
                  "getObject() should have returned a valid EmbeddedCacheManager, even if no defaulConfigurationLocation "
                        + "has been specified. However, it returned null.", cm);
         }
      });
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanEmbeddedCacheManagerFactoryBean#setConfigurationFileLocation(org.springframework.core.io.Resource)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanEmbeddedCacheManagerFactoryBeanShouldCreateACustomizedCacheManagerIfGivenADefaultConfigurationLocation()
         throws Exception {
      final Resource infinispanConfig = new ClassPathResource(NAMED_ASYNC_CACHE_CONFIG_LOCATION,
                                                              getClass());

      final InfinispanEmbeddedCacheManagerFactoryBean objectUnderTest = new TestInfinispanEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setConfigurationFileLocation(infinispanConfig);
      objectUnderTest.afterPropertiesSet();

      withCacheManager(new CacheManagerCallable(objectUnderTest.getObject()) {
         @Override
         public void call() {
            assertNotNull(
                  "getObject() should have returned a valid EmbeddedCacheManager, configured using the configuration file "
                        + "set on SpringEmbeddedCacheManagerFactoryBean. However, it returned null.",
                  cm);
            final Cache<Object, Object> cacheDefinedInCustomConfiguration = cm.getCache(CACHE_NAME_FROM_CONFIGURATION_FILE);
            final Configuration configuration = cacheDefinedInCustomConfiguration.getCacheConfiguration();
            assertEquals(
                  "The cache named ["
                        + CACHE_NAME_FROM_CONFIGURATION_FILE
                        + "] is configured to have asynchonous replication cache mode. Yet, the cache returned from getCache("
                        + CACHE_NAME_FROM_CONFIGURATION_FILE
                        + ") has a different cache mode. Obviously, SpringEmbeddedCacheManagerFactoryBean did not use "
                        + "the configuration file when instantiating EmbeddedCacheManager.",
                  CacheMode.REPL_ASYNC, configuration.clustering().cacheMode());
         }
      });
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanEmbeddedCacheManagerFactoryBean#getObjectType()}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanEmbeddedCacheManagerFactoryBeanShouldReportTheCorrectObjectType()
         throws Exception {
      final InfinispanEmbeddedCacheManagerFactoryBean objectUnderTest = new TestInfinispanEmbeddedCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      withCacheManager(new CacheManagerCallable(objectUnderTest.getObject()) {
         @Override
         public void call() {
            assertEquals(
                  "getObjectType() should return the most derived class of the actual EmbeddedCacheManager "
                        + "implementation returned from getObject(). However, it didn't.",
                  cm.getClass(), objectUnderTest.getObjectType());
         }
      });
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanEmbeddedCacheManagerFactoryBean#isSingleton()}
    * .
    */
   @Test
   public final void infinispanEmbeddedCacheManagerFactoryBeanShouldDeclareItselfToOnlyProduceSingletons() {
      final InfinispanEmbeddedCacheManagerFactoryBean objectUnderTest = new TestInfinispanEmbeddedCacheManagerFactoryBean();

      assertTrue("isSingleton() should always return true. However, it returned false",
                 objectUnderTest.isSingleton());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanEmbeddedCacheManagerFactoryBean#destroy()}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanEmbeddedCacheManagerFactoryBeanShouldStopTheCreateEmbeddedCacheManagerWhenBeingDestroyed()
         throws Exception {
      final InfinispanEmbeddedCacheManagerFactoryBean objectUnderTest = new TestInfinispanEmbeddedCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      final EmbeddedCacheManager embeddedCacheManager = objectUnderTest.getObject();
      embeddedCacheManager.getCache(); // Implicitly starts EmbeddedCacheManager
      objectUnderTest.destroy();

      withCacheManager(new CacheManagerCallable(objectUnderTest.getObject()) {
         @Override
         public void call() {
            assertEquals(
                  "SpringEmbeddedCacheManagerFactoryBean should stop the created EmbeddedCacheManager when being destroyed. "
                        + "However, the created EmbeddedCacheManager is still not terminated.",
                  ComponentStatus.TERMINATED, embeddedCacheManager.getStatus());
         }
      });
   }

   // Testing the addBuilder() methods
   @Test
   public final void testAddConfigurations() throws Exception {
      final InfinispanEmbeddedCacheManagerFactoryBean objectUnderTest = new InfinispanEmbeddedCacheManagerFactoryBean();

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
      final EmbeddedCacheManager infinispanEmbeddedCacheManager = objectUnderTest.getObject();
      assertEquals(infinispanEmbeddedCacheManager.getCacheManagerConfiguration().globalJmxStatistics()
                         .allowDuplicateDomains(), gcb.build().globalJmxStatistics().allowDuplicateDomains());
      assertEquals(infinispanEmbeddedCacheManager.getDefaultCacheConfiguration().transaction()
                         .transactionMode().isTransactional(),
                   builder.build().transaction().transactionMode().isTransactional());
   }
}
