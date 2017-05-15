package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Iterator;
import java.util.stream.IntStream;

import org.apache.lucene.queryparser.classic.ParseException;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.Person;

/**
 * Tests for clustered queries where some of the local indexes are empty
 * @since 9.1
 */
public class ClusteredQueryEmptyIndexTest extends ClusteredQueryTest {

   protected void prepareTestData() {
      IntStream.range(0, NUM_ENTRIES).boxed()
            .map(i -> new Person("name" + i, "blurb" + i, i)).forEach(p -> cacheAMachine1.put(p.getName(), p));

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   @Override
   public void testLocalQuery() throws ParseException {
      super.populateCache();

      final SearchManager searchManager1 = Search.getSearchManager(cacheAMachine1);
      final CacheQuery<?> localQuery1 = searchManager1.getQuery(createLuceneQuery());
      assertEquals(10, localQuery1.getResultSize());

      final SearchManager searchManager2 = Search.getSearchManager(cacheAMachine2);
      final CacheQuery<?> localQuery2 = searchManager2.getQuery(createLuceneQuery());
      assertEquals(0, localQuery2.getResultSize());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }
}
