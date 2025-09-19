package org.infinispan.spring.embedded.config;

import static org.testng.AssertJUnit.assertEquals;

import jakarta.annotation.Resource;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Non transaction cacheable test.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = {"functional", "smoke"}, testName = "spring.embedded.config.NonTransactionalCacheTest")
@ContextConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class,  mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class NonTransactionalCacheTest extends AbstractTestNGSpringContextTests {

   public interface ICachedMock {
      Integer get();
   }

   public static class CachedMock implements ICachedMock {
      private Integer value = 0;

      @Override
      @Cacheable(value = "cachedMock")
      public Integer get() {
         return ++this.value;
      }
   }

   @Resource(name = "mock")
   private ICachedMock mock;

   @Test
   public void testCalls() {
      assertEquals(Integer.valueOf(1), this.mock.get());
      assertEquals(Integer.valueOf(1), this.mock.get());
   }


}
