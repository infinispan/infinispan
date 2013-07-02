package org.infinispan.spring.provider;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringEmbeddedCacheManagerFactoryBean} deployed in a Spring application context.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@Test(testName = "spring.provider.SpringEmbeddedCacheManagerFactoryBeanContextTest", groups = "unit")
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/provider/SpringEmbeddedCacheManagerFactoryBeanContextTest.xml")
public class SpringEmbeddedCacheManagerFactoryBeanContextTest extends
         AbstractTestNGSpringContextTests {

   private static final String SPRING_EMBEDDED_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME = "springEmbeddedCacheManagerWithDefaultConfiguration";

   private static final String SPRING_EMBEDDED_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_FILE_BEAN_NAME = "springEmbeddedCacheManagerConfiguredFromConfigurationFile";

   private static final String SPRING_EMBEDDED_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME = "springEmbeddedCacheManagerConfiguredUsingSetters";

   @Test
   public final void shouldCreateAnEmbeddedCacheManagerWithDefaultSettingsIfNoFurtherConfigurationGiven() {
      final SpringEmbeddedCacheManager springEmbeddedCacheManagerWithDefaultConfiguration = this.applicationContext
               .getBean(SPRING_EMBEDDED_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME,
                        SpringEmbeddedCacheManager.class);

      AssertJUnit
               .assertNotNull(
                        "Spring application context should contain a SpringEmbeddedCacheManager with default settings having bean name = \""
                                 + SPRING_EMBEDDED_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME
                                 + "\". However, it doesn't.",
                        springEmbeddedCacheManagerWithDefaultConfiguration);
      springEmbeddedCacheManagerWithDefaultConfiguration.stop();
   }

   @Test
   public final void shouldCreateAnEmbeddedCacheManagerConfiguredFromConfigurationFileIfConfigurationFileLocationGiven() {
      final SpringEmbeddedCacheManager springEmbeddedCacheManagerConfiguredFromConfigurationFile = this.applicationContext
               .getBean(SPRING_EMBEDDED_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_FILE_BEAN_NAME,
                        SpringEmbeddedCacheManager.class);

      AssertJUnit
               .assertNotNull(
                        "Spring application context should contain a SpringEmbeddedCacheManager configured from configuration file having bean name = \""
                                 + SPRING_EMBEDDED_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_FILE_BEAN_NAME
                                 + "\". However, it doesn't.",
                        springEmbeddedCacheManagerConfiguredFromConfigurationFile);
      springEmbeddedCacheManagerConfiguredFromConfigurationFile.stop();
   }

   @Test
   public final void shouldCreateAnEmbeddedCacheManagerConfiguredUsingPropertiesIfPropertiesAreDefined() {
      final SpringEmbeddedCacheManager springEmbeddedCacheManagerConfiguredUsingProperties = this.applicationContext
               .getBean(SPRING_EMBEDDED_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME,
                        SpringEmbeddedCacheManager.class);

      AssertJUnit
               .assertNotNull(
                        "Spring application context should contain a SpringEmbeddedCacheManager configured using properties having bean name = \""
                                 + SPRING_EMBEDDED_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME
                                 + "\". However, it doesn't.",
                        springEmbeddedCacheManagerConfiguredUsingProperties);
      springEmbeddedCacheManagerConfiguredUsingProperties.stop();
   }
}
