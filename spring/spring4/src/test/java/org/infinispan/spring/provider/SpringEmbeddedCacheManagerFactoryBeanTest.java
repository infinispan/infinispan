package org.infinispan.spring.provider;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.builders.SpringEmbeddedCacheManagerFactoryBeanBuilder;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

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

   private SpringEmbeddedCacheManagerFactoryBean objectUnderTest;

   @AfterTest
   public void closeCacheManager() throws Exception {
      if(objectUnderTest != null) {
         objectUnderTest.destroy();
      }
   }

   @Test
   public void testIfSpringEmbeddedCacheManagerFactoryBeanCreatesACacheManagerEvenIfNoDefaultConfigurationLocationHasBeenSet()
         throws Exception {
      objectUnderTest = SpringEmbeddedCacheManagerFactoryBeanBuilder
            .defaultBuilder().build();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertNotNull(
            "getObject() should have returned a valid SpringEmbeddedCacheManager, even if no defaulConfigurationLocation "
                  + "has been specified. However, it returned null.",
            springEmbeddedCacheManager);
   }

   @Test
   public void testIfSpringEmbeddedCacheManagerFactoryBeanCreatesACustomizedCacheManagerIfGivenADefaultConfigurationLocation()
         throws Exception {
      objectUnderTest = SpringEmbeddedCacheManagerFactoryBeanBuilder
            .defaultBuilder().fromFile(NAMED_ASYNC_CACHE_CONFIG_LOCATION, getClass()).build();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();
      assertNotNull(
            "getObject() should have returned a valid SpringEmbeddedCacheManager, configured using the configuration file "
                  + "set on SpringEmbeddedCacheManagerFactoryBean. However, it returned null.",
            springEmbeddedCacheManager);
      final SpringCache cacheDefinedInCustomConfiguration = springEmbeddedCacheManager
            .getCache(CACHE_NAME_FROM_CONFIGURATION_FILE);
      final org.infinispan.configuration.cache.Configuration configuration = ((Cache) cacheDefinedInCustomConfiguration.getNativeCache())
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
   }

   @Test
   public void testIfSpringEmbeddedCacheManagerFactoryBeanReportsTheCorrectObjectType()
         throws Exception {
      objectUnderTest = SpringEmbeddedCacheManagerFactoryBeanBuilder
            .defaultBuilder().build();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
            "getObjectType() should return the most derived class of the actual SpringEmbeddedCacheManager "
                  + "implementation returned from getObject(). However, it didn't.",
            springEmbeddedCacheManager.getClass(), objectUnderTest.getObjectType());
   }

   @Test
   public void testIfSpringEmbeddedCacheManagerFactoryBeanDeclaresItselfToOnlyProduceSingletons() {
      objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();

      assertTrue("isSingleton() should always return true. However, it returned false",
                 objectUnderTest.isSingleton());
   }

   @Test
   public void testIfSpringEmbeddedCacheManagerFactoryBeanStopsTheCreatedEmbeddedCacheManagerWhenBeingDestroyed()
         throws Exception {
      objectUnderTest = SpringEmbeddedCacheManagerFactoryBeanBuilder
            .defaultBuilder().build();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();
      springEmbeddedCacheManager.getCache("default"); // Implicitly starts
      // SpringEmbeddedCacheManager
      objectUnderTest.destroy();

      assertEquals(
            "SpringEmbeddedCacheManagerFactoryBean should stop the created SpringEmbeddedCacheManager when being destroyed. "
                  + "However, the created SpringEmbeddedCacheManager is still not terminated.",
            ComponentStatus.TERMINATED, springEmbeddedCacheManager.getNativeCacheManager()
                  .getStatus());
   }

   @Test
   public void testIfSpringEmbeddedCacheManagerFactoryBeanAllowesOverridingGlobalConfiguration() throws Exception {
      GlobalConfigurationBuilder overriddenConfiguration = new GlobalConfigurationBuilder();
      overriddenConfiguration.transport().rackId("r2");
      overriddenConfiguration.globalJmxStatistics().allowDuplicateDomains(true);

      objectUnderTest = SpringEmbeddedCacheManagerFactoryBeanBuilder
            .defaultBuilder().fromFile(NAMED_ASYNC_CACHE_CONFIG_LOCATION, getClass())
            .withGlobalConfiguration(overriddenConfiguration).build();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
            "Transport for cache configured in"
            + CACHE_NAME_FROM_CONFIGURATION_FILE + "is assigned to r1 rack. But later Global Configuration overrides "
            + "this setting to r2. Obviously created SpringEmbeddedCacheManagerFactoryBean does not support this kind "
            + "of overriding.",
            "r2",
            springEmbeddedCacheManager.getNativeCacheManager().getCacheManagerConfiguration().transport().rackId());
   }

   @Test
   public void testIfSpringEmbeddedCacheManagerFactoryBeanAllowesOverridingConfigurationBuilder() throws Exception {
      ConfigurationBuilder overriddenBuilder = new ConfigurationBuilder();
      overriddenBuilder.locking().concurrencyLevel(100);

      objectUnderTest = SpringEmbeddedCacheManagerFactoryBeanBuilder
            .defaultBuilder().fromFile(NAMED_ASYNC_CACHE_CONFIG_LOCATION, getClass())
            .withConfigurationBuilder(overriddenBuilder).build();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
            "Concurrency value of LockingLocking for cache configured in"
                  + CACHE_NAME_FROM_CONFIGURATION_FILE + "is equal to 5000. But later Configuration Builder overrides "
                  + "this setting to 100. Obviously created SpringEmbeddedCacheManagerFactoryBean does not support "
                  + "this kind of overriding.",
            100,
            springEmbeddedCacheManager.getNativeCacheManager().getDefaultCacheConfiguration().locking()
                  .concurrencyLevel());
   }

   @Test
   public void testIfSpringEmbeddedCacheManagerFactoryBeanAllowesOverridingConfigurationWithEmptyInputStream()
         throws Exception {
      objectUnderTest = SpringEmbeddedCacheManagerFactoryBeanBuilder
            .defaultBuilder().build();

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
                         .allowDuplicateDomains(), true);
      assertEquals(infinispanEmbeddedCacheManager.getDefaultCacheConfiguration().transaction().transactionMode().isTransactional(),
                   false);
   }
}
