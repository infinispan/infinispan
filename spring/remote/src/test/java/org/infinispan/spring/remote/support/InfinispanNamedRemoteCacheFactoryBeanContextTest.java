package org.infinispan.spring.remote.support;

import org.infinispan.Cache;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import static org.testng.AssertJUnit.assertNotNull;

/**
 * <p>
 * Test {@link InfinispanNamedRemoteCacheFactoryBean} deployed in a Spring application context.
 * </p>
 *
 * @author Olaf Bergner
 *
 */
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/remote/support/InfinispanNamedRemoteCacheFactoryBeanContextTest.xml")
//@Test(testName = "spring.support.remote.InfinispanNamedRemoteCacheFactoryBeanContextTest", groups = "functional")
@TestExecutionListeners(value = InfinispanTestExecutionListener.class,  mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class InfinispanNamedRemoteCacheFactoryBeanContextTest extends AbstractTestNGSpringContextTests {

   private static final String INFINISPAN_NAMED_REMOTE_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME = "infinispanNamedRemoteCacheWithoutFurtherConfiguration";

//   @Test
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
