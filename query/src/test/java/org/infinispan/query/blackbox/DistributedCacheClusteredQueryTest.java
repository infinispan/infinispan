package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
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
      final Query<Object> normalQueryA = cache(0).query(queryString);
      final Query<Object> normalQueryB = cache(1).query(queryString);

      assertEquals(10, normalQueryA.execute().count().value());
      assertEquals(10, normalQueryB.execute().count().value());

      List<?> results1 = normalQueryA.local(true).execute().list();
      List<?> results2 = normalQueryB.local(true).execute().list();

      assertEquals(10, results1.size() + results2.size());
   }

   @Test
   public void testNonIndexedQueryLocalOnly() {
      String q = "FROM org.infinispan.query.test.Person p where p.nonIndexedField = 'na'";

      List<?> results1 = cache(0).query(q).local(true).execute().list();
      List<?> results2 = cache(1).query(q).local(true).execute().list();

      assertEquals(NUM_ENTRIES, results1.size() + results2.size());

      q = "SELECT COUNT(nonIndexedField) FROM org.infinispan.query.test.Person GROUP BY nonIndexedField";

      results1 = cache(0).query(q).local(true).execute().list();
      results2 = cache(1).query(q).local(true).execute().list();

      final Object[] row1 = (Object[]) results1.get(0);
      final Object[] row2 = (Object[]) results2.get(0);

      assertEquals(NUM_ENTRIES, (long) row1[0] + (long) row2[0]);
   }
}
