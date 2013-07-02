package org.infinispan.spring.support.embedded;

import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.Cache;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link InfinispanNamedEmbeddedCacheFactoryBean} deployed in a Spring application context.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/support/embedded/InfinispanNamedEmbeddedCacheFactoryBeanContextTest.xml")
@Test(testName = "spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBeanContextTest", groups = "unit")
public class InfinispanNamedEmbeddedCacheFactoryBeanContextTest extends
         AbstractTestNGSpringContextTests {

   private static final String INFINISPAN_NAMED_EMBEDDED_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME = "infinispanNamedEmbeddedCacheWithoutFurtherConfiguration";

   private static final String INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NONE_BEAN_NAME = "infinispanNamedEmbeddedCacheConfiguredUsingModeNONE";

   private static final String INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_DEFAULT_BEAN_NAME = "infinispanNamedEmbeddedCacheConfiguredUsingModeDEFAULT";

   private static final String INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NAMED_BEAN_NAME = "infinispanNamedEmbeddedCacheConfiguredUsingModeNAMED";

   @Test
   public final void shouldCreateAnEmbeddedCacheWithDefaultSettingsIfNoFurtherConfigurationGiven() {
      final Cache<Object, Object> infinispanNamedEmbeddedCacheWithoutFurtherConfiguration = this.applicationContext
               .getBean(INFINISPAN_NAMED_EMBEDDED_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME,
                        Cache.class);

      assertNotNull(
               "Spring application context should contain a named Infinispan cache having bean name = \""
                        + INFINISPAN_NAMED_EMBEDDED_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME
                        + "\". However, it doesn't.",
               infinispanNamedEmbeddedCacheWithoutFurtherConfiguration);
   }

   @Test
   public final void shouldCreateAnEmbeddedCacheConfiguredUsingConfigurationModeNONE() {
      final Cache<Object, Object> infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeNone = this.applicationContext
               .getBean(INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NONE_BEAN_NAME,
                        Cache.class);

      assertNotNull(
               "Spring application context should contain a named Infinispan cache having bean name = \""
                        + INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NONE_BEAN_NAME
                        + "\" that has been configured using configuration mode NONE. However, it doesn't.",
               infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeNone);
   }

   @Test
   public final void shouldCreateAnEmbeddedCacheConfiguredUsingConfigurationModeDEFAULT() {
      final Cache<Object, Object> infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeDefault = this.applicationContext
               .getBean(INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_DEFAULT_BEAN_NAME,
                        Cache.class);

      assertNotNull(
               "Spring application context should contain a named Infinispan cache having bean name = \""
                        + INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_DEFAULT_BEAN_NAME
                        + "\" that has been configured using configuration mode DEFAULT. However, it doesn't.",
               infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeDefault);
   }

   @Test
   public final void shouldCreateAnEmbeddedCacheConfiguredUsingConfigurationModeNAMED() {
      final Cache<Object, Object> infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeNamed = this.applicationContext
               .getBean(INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NAMED_BEAN_NAME,
                        Cache.class);

      assertNotNull(
               "Spring application context should contain a named Infinispan cache having bean name = \""
                        + INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NAMED_BEAN_NAME
                        + "\" that has been configured using configuration mode NAMED. However, it doesn't.",
               infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeNamed);
   }

}
