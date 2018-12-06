package org.infinispan.spring.embedded.support;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertSame;

import org.infinispan.Cache;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link org.infinispan.spring.embedded.InfinispanDefaultCacheFactoryBean} deployed in a Spring application context.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 *
 */
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/embedded/support/InfinispanDefaultCacheFactoryBeanContextTest.xml")
@Test(testName = "spring.embedded.support.InfinispanDefaultCacheFactoryBeanContextTest", groups = "unit")
@TestExecutionListeners(InfinispanTestExecutionListener.class)
public class InfinispanDefaultCacheFactoryBeanContextTest extends AbstractTestNGSpringContextTests {

   private static final String DEFAULT_CACHE_NAME = "testDefaultCache";

   @Test
   public final void shouldProduceANonNullCache() {
      final Cache<Object, Object> testDefaultCache = this.applicationContext.getBean(
            DEFAULT_CACHE_NAME, Cache.class);

      assertNotNull(
            "Spring application context should contain an Infinispan cache under the bean name \""
                  + DEFAULT_CACHE_NAME + "\". However, it doesn't.", testDefaultCache);
   }

   @Test
   public final void shouldAlwaysReturnTheSameCache() {
      final Cache<Object, Object> testDefaultCache1 = this.applicationContext.getBean(
            DEFAULT_CACHE_NAME, Cache.class);
      final Cache<Object, Object> testDefaultCache2 = this.applicationContext.getBean(
            DEFAULT_CACHE_NAME, Cache.class);

      assertSame(
            "InfinispanDefaultCacheFactoryBean should always return the same cache instance when being "
                  + "called repeatedly. However, the cache instances are not the same.",
            testDefaultCache1, testDefaultCache2);
   }
}
