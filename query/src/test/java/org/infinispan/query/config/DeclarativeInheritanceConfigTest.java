package org.infinispan.query.config;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Set;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.indexedembedded.Book;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(testName = "query.config.DeclarativeInheritanceConfigTest", groups = "functional")
public class DeclarativeInheritanceConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.fromXml("configuration-inheritance-parsing-test.xml");
   }

   public void testIndexedConfigurationInheritance() {
      Configuration configuration = cacheManager.getCacheConfiguration("default");
      Set<String> indexedEntities = configuration.indexing().indexedEntityTypes();
      assertEquals(1, indexedEntities.size());
      assertTrue(indexedEntities.contains(Book.class.getName()));

      configuration = cacheManager.getCacheConfiguration("extended");
      indexedEntities = configuration.indexing().indexedEntityTypes();
      assertEquals(1, indexedEntities.size());
      assertTrue(indexedEntities.contains(AnotherGrassEater.class.getName()));
   }
}
