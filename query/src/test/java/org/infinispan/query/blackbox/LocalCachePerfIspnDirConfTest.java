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
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.query.helper.TestQueryHelperFactory.createCacheQuery;

/**
 * Tests the functionality of the queries in case when the NRT index manager is used in combination with ISPN Directory.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCachePerfIspnDirConfTest")
public class LocalCachePerfIspnDirConfTest extends LocalCacheTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.fromXml("nrt-performance-writer-infinispandirectory.xml");
      cache = cacheManager.getCache("Indexed");

      return cacheManager;
   }
}
