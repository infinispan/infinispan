package org.infinispan.compatibility.adaptor52x;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "compatibility.loaders.FileParsingTest")
public class FileParsingTest extends SingleCacheManagerTest {

   public static final String LOCATION = "__tmp_to_del___";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return new DefaultCacheManager("52x-custom-loader.xml");
   }

   @Override
   protected void teardown() {
      super.teardown();
      TestingUtil.recursiveFileRemove(LOCATION);
   }

   public void testConfiguration() {
      Cache<Object,Object> customLoaderCache = cacheManager.getCache("customLoaderCache");
      List<StoreConfiguration> loaders = customLoaderCache.getCacheConfiguration().persistence().stores();
      assertEquals(loaders.size(), 1);

   }
}
