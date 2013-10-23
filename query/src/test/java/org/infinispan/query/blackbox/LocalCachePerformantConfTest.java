package org.infinispan.query.blackbox;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static org.infinispan.query.helper.TestQueryHelperFactory.createCacheQuery;

/**
 * Tests the functionality of the queries in case when the NRT index manager is used in combination with FileStore.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCachePerformantConfTest")
public class LocalCachePerformantConfTest extends LocalCacheTest {

   private String directoryName = "/tunedConfDir";
   private String indexDirectory = null;

   public LocalCachePerformantConfTest() {
      indexDirectory = System.getProperty("java.io.tmpdir") + directoryName;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.fromXml("nrt-performance-writer.xml");
      cache = cacheManager.getCache("Indexed");

      return cacheManager;
   }

   @BeforeMethod
   protected void createDirectory() {
      new File(indexDirectory).mkdirs();
   }

   @Override
   @AfterMethod
   protected void destroyAfterMethod() {
      try {
         //first stop cache managers, then clear the index
         super.destroyAfterMethod();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         TestingUtil.recursiveFileRemove(indexDirectory);
      }
   }
}
