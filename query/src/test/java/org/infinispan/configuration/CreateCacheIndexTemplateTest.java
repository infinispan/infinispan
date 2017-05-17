package org.infinispan.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.CreateCacheIndexTemplateTest")
public class CreateCacheIndexTemplateTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().index(Index.ALL);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void createCacheTest() {
      ConfigurationBuilder builder = new ConfigurationBuilder();

      builder.read(cacheManager.getDefaultCacheConfiguration());
      builder.template(false);

      cacheManager.defineConfiguration("newCache", builder.build());

      cacheManager.getCache("newCache");
   }


}
