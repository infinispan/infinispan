package org.infinispan.query.distributed;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

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
      CacheQuery cacheQuery = searchManager.getClusteredQuery(new TermQuery(new Term("make", carMake)));
      assertEquals(expectedCount, cacheQuery.getResultSize());
   }
}
