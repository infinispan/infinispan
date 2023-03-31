package org.infinispan.query.config;


import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.config.IndexingConfigurationIgnoredTest")
public class IndexingConfigurationIgnoredTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager manager;

   @Test
   public void testIndexingParametersForNamedCache() {
      Cache<Object, Object> inMemory = manager.getCache("memory-searchable");
      inMemory.start();
   }

   @BeforeMethod
   public void init() throws Exception {
      manager = TestCacheManagerFactory.fromXml("configuration-parsing-test.xml");
   }

   @AfterMethod
   public void destroy() throws Exception {
      TestingUtil.killCacheManagers(manager);
   }

}
