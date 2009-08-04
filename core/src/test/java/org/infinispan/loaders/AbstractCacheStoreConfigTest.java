package org.infinispan.loaders;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests that cover {@link  AbstractCacheStoreConfig }
 *
 * @author Adrian Cole
 * @since 4.0
 */
@Test(groups = "unit", testName = "loaders.AbstractCacheStoreConfigTest")
public class AbstractCacheStoreConfigTest {
   private AbstractCacheStoreConfig config;

   @BeforeMethod
   public void setUp() throws Exception {
      config = new AbstractCacheStoreConfig();
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      config = null;
   }

   @Test
   public void testIsPurgeSynchronously() {
      assert !config.isPurgeSynchronously();
   }

   @Test
   public void testSetPurgeSynchronously() {
      config.setPurgeSynchronously(true);
      assert config.isPurgeSynchronously();
   }
}