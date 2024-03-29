package org.infinispan.query.dsl.embedded;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.List;
import java.util.Random;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.query.core.impl.QueryCache;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author Matej Cimbora
 * @author anistor@gmail.com
 * @since 9.0
 */
@Test(groups = "profiling", testName = "query.dsl.embedded.NamedParamsPerfTest")
public class NamedParamsPerfTest extends AbstractQueryDslTest {

   private static final int ITERATIONS = 1000;

   @Indexed
   static class Person {

      @Basic(projectable = true, sortable = true)
      final int id;

      @Basic(projectable = true, sortable = true, indexNullAs = "_null_")
      final String firstName;

      @Basic(projectable = true, sortable = true, indexNullAs = "_null_")
      final String lastName;

      Person(int id, String firstName, String lastName) {
         this.id = id;
         this.firstName = firstName;
         this.lastName = lastName;
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
      createClusteredCaches(1, cfg);
   }

   public void testNamedParamPerfComparison() {
      QueryFactory factory = getQueryFactory();

      String[] fnames = {"Matej", "Roman", "Jakub", "Jiri", "Anna", "Martin", "Vojta", "Alan"};
      String[] lnames = {"Cimbora", "Macor", "Markos", "Holusa", "Manukyan", "Gencur", "Vrabel", "Juranek", "Field"};

      Random random = new Random(3);
      for (int i = 0; i < 999; i++) {
         cache(0).put(i, new Person(i, fnames[random.nextInt(fnames.length)], lnames[random.nextInt(lnames.length)]));
      }
      cache(0).put(1000, new Person(999, "Unnamed", "Unnamed"));

      QueryCache queryCache = GlobalComponentRegistry.componentOf(manager(0), QueryCache.class);
      assertNotNull(queryCache);

      Query<Person> query = factory.create("FROM " + Person.class.getName() + " WHERE firstName = :nameParam1 OR lastName = :nameParam2 OR id >= :idParam1 OR id < :idParam2");

      long t1 = 0;
      long t2 = 0;
      long t3 = 0;

      for (int i = 0; i < ITERATIONS; i++) {
         queryCache.clear();

         long start = System.nanoTime();
         query.setParameter("nameParam1", "Unnamed")
               .setParameter("nameParam2", "ww")
               .setParameter("idParam1", 1000)
               .setParameter("idParam2", 0);
         List<Person> list = query.execute().list();
         t1 += (System.nanoTime() - start);  // first run is expected to take much longer than subsequent runs
         assertEquals(1, list.size());

         start = System.nanoTime();
         query.setParameter("nameParam1", "Unnamed")
               .setParameter("nameParam2", "zz")
               .setParameter("idParam1", 2000)
               .setParameter("idParam2", -1000);
         list = query.execute().list();
         t2 += (System.nanoTime() - start);
         assertEquals(1, list.size());

         start = System.nanoTime();
         query.setParameter("nameParam1", "Unnamed")
               .setParameter("nameParam2", "bb")
               .setParameter("idParam1", 5000)
               .setParameter("idParam2", -3000);
         list = query.execute().list();
         t3 += (System.nanoTime() - start);
         assertEquals(1, list.size());
      }

      System.out.println("NamedParamsPerfTest.testNamedParamPerfComparison t1 (avg, us) = " + (t1 / 1000.0 / ITERATIONS));
      System.out.println("NamedParamsPerfTest.testNamedParamPerfComparison t2 (avg, us) = " + (t2 / 1000.0 / ITERATIONS));
      System.out.println("NamedParamsPerfTest.testNamedParamPerfComparison t3 (avg, us) = " + (t3 / 1000.0 / ITERATIONS));
   }
}
