package org.infinispan.query.dsl.embedded;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.List;
import java.util.Random;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
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

   @Indexed
   public static class Person {

      @Field(store = Store.YES, analyze = Analyze.NO)
      @SortableField
      final int id;

      @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
      @SortableField
      final String firstName;

      @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
      @SortableField
      final String lastName;

      public Person(int id, String firstName, String lastName) {
         this.id = id;
         this.firstName = firstName;
         this.lastName = lastName;
      }

      public String getFirstName() {
         return firstName;
      }

      public String getLastName() {
         return lastName;
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(1, cfg);
   }

   public void testNamedParamPerfComparison() throws Exception {
      QueryFactory factory = getQueryFactory();

      String[] fnames = {"Matej", "Roman", "Jakub", "Jiri", "Anna", "Martin", "Vojta", "Alan"};
      String[] lnames = {"Cimbora", "Macor", "Markos", "Holusa", "Manukyan", "Gencur", "Vrabel", "Juranek", "Field"};

      Random random = new Random(3);
      for (int i = 0; i < 999; i++) {
         cache(0).put(i, new Person(i, fnames[random.nextInt(fnames.length)], lnames[random.nextInt(lnames.length)]));
      }
      cache(0).put(1000, new Person(999, "Unnamed", "Unnamed"));

      QueryCache queryCache = manager(0).getGlobalComponentRegistry().getComponent(QueryCache.class);
      assertNotNull(queryCache);

      Query query = factory.from(Person.class)
            .having("firstName").eq(Expression.param("nameParam1"))
            .or()
            .having("lastName").eq(Expression.param("nameParam2"))
            .or()
            .having("id").gte(Expression.param("idParam1"))
            .or()
            .having("id").lt(Expression.param("idParam2"))
            .build();

      final int iterations = 1000;
      long t1 = 0;
      long t2 = 0;
      long t3 = 0;

      for (int i = 0; i < iterations; i++) {
         queryCache.clear();

         long start = System.nanoTime();
         query.setParameter("nameParam1", "Unnamed")
               .setParameter("nameParam2", "ww")
               .setParameter("idParam1", 1000)
               .setParameter("idParam2", 0);
         List<Object> list = query.list();
         long duration = System.nanoTime() - start;  // first run is expected to take much longer than subsequent runs
         assertEquals(1, list.size());
         t1 += duration;

         start = System.nanoTime();
         query.setParameter("nameParam1", "Unnamed")
               .setParameter("nameParam2", "zz")
               .setParameter("idParam1", 2000)
               .setParameter("idParam2", -1000);
         list = query.list();
         duration = System.nanoTime() - start;
         assertEquals(1, list.size());
         t2 += duration;

         start = System.nanoTime();
         query.setParameter("nameParam1", "Unnamed")
               .setParameter("nameParam2", "bb")
               .setParameter("idParam1", 5000)
               .setParameter("idParam2", -3000);
         list = query.list();
         duration = System.nanoTime() - start;
         assertEquals(1, list.size());
         t3 += duration;
      }

      System.out.println("NamedParamsPerfTest.testNamedParamPerfComparison t1 (avg, us) = " + (t1 / 1000.0 / iterations));
      System.out.println("NamedParamsPerfTest.testNamedParamPerfComparison t2 (avg, us) = " + (t2 / 1000.0 / iterations));
      System.out.println("NamedParamsPerfTest.testNamedParamPerfComparison t3 (avg, us) = " + (t3 / 1000.0 / iterations));
   }
}
