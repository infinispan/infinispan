package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.testng.annotations.Test;

/**
 * Testing Clustered distributed queries on Distributed  cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.DistributedCacheClusteredQueryTest")
public class DistributedCacheClusteredQueryTest extends ClusteredQueryTest {

   public CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }

   @Override
   protected int numOwners() {
      return 1;
   }

   @Test
   public void testIndexedQueryLocalOnly() {
      QueryFactory queryFactoryA = Search.getQueryFactory(cache(0));
      QueryFactory queryFactoryB = Search.getQueryFactory(cache(1));

      final Query<Object> normalQueryA = queryFactoryA.create(queryString);
      final Query<Object> normalQueryB = queryFactoryB.create(queryString);

      assertEquals(10, normalQueryA.execute().hitCount().orElse(-1));
      assertEquals(10, normalQueryB.execute().hitCount().orElse(-1));

      List<?> results1 = normalQueryA.local(true).execute().list();
      List<?> results2 = normalQueryB.local(true).execute().list();

      assertEquals(10, results1.size() + results2.size());
   }

   @Test
   public void testNonIndexedQueryLocalOnly() {
      String q = "FROM org.infinispan.query.test.Person p where p.nonIndexedField = 'na'";

      QueryFactory queryFactoryA = Search.getQueryFactory(cache(0));
      QueryFactory queryFactoryB = Search.getQueryFactory(cache(1));

      List<?> results1 = queryFactoryA.create(q).local(true).execute().list();
      List<?> results2 = queryFactoryB.create(q).local(true).execute().list();

      assertEquals(NUM_ENTRIES, results1.size() + results2.size());

      q = "SELECT COUNT(nonIndexedField) FROM org.infinispan.query.test.Person GROUP BY nonIndexedField";

      results1 = queryFactoryA.create(q).local(true).execute().list();
      results2 = queryFactoryB.create(q).local(true).execute().list();

      final Object[] row1 = (Object[]) results1.get(0);
      final Object[] row2 = (Object[]) results2.get(0);

      assertEquals(NUM_ENTRIES, (long) row1[0] + (long) row2[0]);
   }
}
