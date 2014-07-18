package org.infinispan.spring.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.fail;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "spring.config.CacheLoaderNotFoundTest")
@ContextConfiguration
public class CacheLoaderNotFoundTest extends AbstractTestNGSpringContextTests {

   @Autowired
   @Qualifier("cacheManager")
   private CacheManager cm;

   @BeforeClass
   @Override
   protected void springTestContextPrepareTestInstance() throws Exception {
      try {
         super.springTestContextPrepareTestInstance();
         fail("Show have thrown an error indicating issues with the cache loader");
      } catch (IllegalStateException e) {
      }
   }

   @Test
   public void testCacheManagerExists() {
      Assert.assertNull(cm);
   }

}
