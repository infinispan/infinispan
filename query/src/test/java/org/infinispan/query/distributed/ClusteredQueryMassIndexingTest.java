package org.infinispan.query.distributed;

import static org.testng.AssertJUnit.assertEquals;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.testng.annotations.Test;

/**
 * Tests verifying that the Mass Indexing works for Clustered queries as well.
 */
@Test(groups = "functional", testName = "query.distributed.ClusteredQueryMassIndexingTest")
public class ClusteredQueryMassIndexingTest extends DistributedMassIndexingTest {

   @Override
   protected String getConfigurationFile() {
      return "unshared-indexing-distribution.xml";
   }

   protected void verifyFindsCar(Cache cache, int expectedCount, String carMake) {
      CacheQuery<?> cacheQuery = Search.getSearchManager(cache)
            .getClusteredQuery(new TermQuery(new Term("make", carMake)));

      assertEquals(expectedCount, cacheQuery.getResultSize());
   }

}
