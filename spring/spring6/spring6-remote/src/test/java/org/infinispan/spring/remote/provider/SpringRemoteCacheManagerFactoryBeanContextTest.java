package org.infinispan.spring.remote.provider;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringRemoteCacheManagerFactoryBean} deployed in a Spring application context.
 * </p>
 *
 * @author Olaf Bergner
 *
 */
@Test(testName = "spring.provider.SpringRemoteCacheManagerFactoryBeanContextTest", groups = "unit")
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/remote/provider/SpringRemoteCacheManagerFactoryBeanContextTest.xml")
@TestExecutionListeners(value = InfinispanTestExecutionListener.class,  mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class SpringRemoteCacheManagerFactoryBeanContextTest extends AbstractTestNGSpringContextTests {

   private static final String SPRING_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME = "springRemoteCacheManagerWithDefaultConfiguration";

   private static final String SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME = "springRemoteCacheManagerConfiguredFromConfigurationPropertiesFile";

   private static final String SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_CONFIGURATION_PROPERTIES_BEAN_NAME = "springRemoteCacheManagerConfiguredUsingConfigurationProperties";

   private static final String SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME = "springRemoteCacheManagerConfiguredUsingSetters";

   @Test
   public final void shouldCreateARemoteCacheManagerWithDefaultSettingsIfNoFurtherConfigurationGiven() {
      final SpringRemoteCacheManager springRemoteCacheManagerWithDefaultConfiguration = this.applicationContext
            .getBean(SPRING_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME,
                     SpringRemoteCacheManager.class);

      assertNotNull(
            "Spring application context should contain a SpringRemoteCacheManager with default settings having bean name = \""
                  + SPRING_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME
                  + "\". However, it doesn't.",
            springRemoteCacheManagerWithDefaultConfiguration);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredFromConfigurationFileIfConfigurationFileLocationGiven() {
      final SpringRemoteCacheManager springRemoteCacheManagerConfiguredFromConfigurationFile = this.applicationContext
            .getBean(SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME,
                     SpringRemoteCacheManager.class);

      assertNotNull(
            "Spring application context should contain a SpringRemoteCacheManager configured from configuration file having bean name = \""
                  + SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME
                  + "\". However, it doesn't.",
            springRemoteCacheManagerConfiguredFromConfigurationFile);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredUsingConfigurationPropertiesSetInApplicationContext() {
      final SpringRemoteCacheManager springRemoteCacheManagerConfiguredUsingConfigurationProperties = this.applicationContext
            .getBean(SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_CONFIGURATION_PROPERTIES_BEAN_NAME,
                     SpringRemoteCacheManager.class);

      assertNotNull(
            "Spring application context should contain a SpringRemoteCacheManager configured using configuration properties set in application context having bean name = \""
                  + SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_CONFIGURATION_PROPERTIES_BEAN_NAME
                  + "\". However, it doesn't.",
            springRemoteCacheManagerConfiguredUsingConfigurationProperties);
      assertEquals(500, springRemoteCacheManagerConfiguredUsingConfigurationProperties.getReadTimeout());
      assertEquals(700, springRemoteCacheManagerConfiguredUsingConfigurationProperties.getWriteTimeout());
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredUsingSettersIfPropertiesAreDefined() {
      final SpringRemoteCacheManager springRemoteCacheManagerConfiguredUsingSetters = this.applicationContext
            .getBean(SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME,
                     SpringRemoteCacheManager.class);

      assertNotNull(
            "Spring application context should contain a SpringRemoteCacheManager configured using properties having bean name = \""
                  + SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME
                  + "\". However, it doesn't.",
            springRemoteCacheManagerConfiguredUsingSetters);
   }
}
