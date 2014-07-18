package org.infinispan.spring.support.remote;

import org.infinispan.Cache;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import static org.testng.AssertJUnit.assertNotNull;

/**
 * <p>
 * Test {@link InfinispanNamedRemoteCacheFactoryBean} deployed in a Spring application context.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 *
 */
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/support/remote/InfinispanNamedRemoteCacheFactoryBeanContextTest.xml")
// @Test(testName = "spring.support.remote.InfinispanNamedRemoteCacheFactoryBeanContextTest", groups
// = "functional")
public class InfinispanNamedRemoteCacheFactoryBeanContextTest extends
                                                              AbstractTestNGSpringContextTests {

   private static final String INFINISPAN_NAMED_REMOTE_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME = "infinispanNamedRemoteCacheWithoutFurtherConfiguration";

   // @Test
   public final void shouldCreateARemoteCacheWithDefaultSettingsIfNoFurtherConfigurationGiven() {
      final Cache<Object, Object> infinispanNamedRemoteCacheWithoutFurtherConfiguration = this.applicationContext
            .getBean(INFINISPAN_NAMED_REMOTE_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME,
                     Cache.class);

      assertNotNull(
            "Spring application context should contain a named Infinispan cache having bean name = \""
                  + INFINISPAN_NAMED_REMOTE_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME
                  + "\". However, it doesn't.",
            infinispanNamedRemoteCacheWithoutFurtherConfiguration);
   }
}
