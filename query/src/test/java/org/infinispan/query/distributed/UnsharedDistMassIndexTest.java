package org.infinispan.query.distributed;

import static org.testng.AssertJUnit.assertEquals;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.testng.annotations.Test;

/**
 * Test for MassIndexer on DIST caches with unshared infinispan indexes
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.UnsharedDistMassIndexTest")
public class UnsharedDistMassIndexTest extends DistributedMassIndexingTest {

   @Override
   protected String getConfigurationFile() {
      return "unshared-indexing-distribution.xml";
   }

   @Override
   protected void verifyFindsCar(Cache cache, int expectedCount, String carMake) {
      SearchManager searchManager = Search.getSearchManager(cache);
      CacheQuery<?> cacheQuery = searchManager.getQuery(new TermQuery(new Term("make", carMake)), IndexedQueryMode.BROADCAST);
      assertEquals(expectedCount, cacheQuery.getResultSize());
   }
}
