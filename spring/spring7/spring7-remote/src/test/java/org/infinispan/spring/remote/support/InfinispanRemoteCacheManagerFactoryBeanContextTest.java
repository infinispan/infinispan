package org.infinispan.spring.remote.support;

import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link InfinispanRemoteCacheManagerFactoryBean} deployed in a Spring application context.
 * </p>
 *
 * @author Olaf Bergner
 *
 */
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/remote/support/InfinispanRemoteCacheManagerFactoryBeanContextTest.xml")
@Test(testName = "spring.support.remote.InfinispanRemoteCacheManagerFactoryBeanContextTest", groups = "unit")
@TestExecutionListeners(value = InfinispanTestExecutionListener.class,  mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class InfinispanRemoteCacheManagerFactoryBeanContextTest extends AbstractTestNGSpringContextTests {

   private static final String INFINISPAN_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME = "infinispanRemoteCacheManagerWithDefaultConfiguration";

   private static final String INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME = "infinispanRemoteCacheManagerConfiguredFromConfigurationPropertiesFile";

   private static final String INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_BEAN_NAME = "infinispanRemoteCacheManagerConfiguredFromConfigurationProperties";

   private static final String INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME = "infinispanRemoteCacheManagerConfiguredUsingSetters";

   @Test
   public final void shouldCreateARemoteCacheManagerWithDefaultSettingsIfNoFurtherConfigurationGiven() {
      final RemoteCacheManager infinispanRemoteCacheManagerWithDefaultConfiguration = this.applicationContext
            .getBean(INFINISPAN_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME,
                     RemoteCacheManager.class);

      assertNotNull(
            "Spring application context should contain a RemoteCacheManager with default settings having bean name = \""
                  + INFINISPAN_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME
                  + "\". However, it doesn't.",
            infinispanRemoteCacheManagerWithDefaultConfiguration);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredFromConfigurationPropertiesFileIfConfigurationPropertiesFileLocationGiven() {
      final RemoteCacheManager infinispanRemoteCacheManagerConfiguredFromConfigurationFile = this.applicationContext
            .getBean(INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME,
                     RemoteCacheManager.class);

      assertNotNull(
            "Spring application context should contain a RemoteCacheManager configured from configuration properties file having bean name = \""
                  + INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME
                  + "\". However, it doesn't.",
            infinispanRemoteCacheManagerConfiguredFromConfigurationFile);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredFromConfigurationPropertiesIfConfigurationPropertiesGiven() {
      final RemoteCacheManager infinispanRemoteCacheManagerConfiguredFromConfigurationProperties = this.applicationContext
            .getBean(INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_BEAN_NAME,
                     RemoteCacheManager.class);

      assertNotNull(
            "Spring application context should contain a RemoteCacheManager configured from configuration properties having bean name = \""
                  + INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_BEAN_NAME
                  + "\". However, it doesn't.",
            infinispanRemoteCacheManagerConfiguredFromConfigurationProperties);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredUsingSettersIfPropertiesAreDefined() {
      final RemoteCacheManager infinispanRemoteCacheManagerConfiguredUsingSetters = this.applicationContext
            .getBean(INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME,
                     RemoteCacheManager.class);

      assertNotNull(
            "Spring application context should contain a SpringRemoteCacheManager configured using properties having bean name = \""
                  + INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME
                  + "\". However, it doesn't.",
            infinispanRemoteCacheManagerConfiguredUsingSetters);
   }
}
