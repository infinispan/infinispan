package org.infinispan.spring.config;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import javax.annotation.Resource;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Non transaction cacheable test.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "spring.config.NonTransactionalCacheTest")
@ContextConfiguration
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
