package org.infinispan.objectfilter.test;

import org.infinispan.objectfilter.BaseMatcher;
import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.test.model.Person;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class AbstractMatcherTest {

   protected abstract Object createPerson() throws Exception;

   protected abstract BaseMatcher createMatcher();

   protected boolean match(String queryString, Object obj) throws Exception {
      BaseMatcher matcher = createMatcher();

      final int[] matchCount = new int[1];

      matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection, boolean isMatching) {
            if (isMatching) {
               matchCount[0]++;
            }
         }
      });

      matcher.match(obj);
      return matchCount[0] == 1;
   }

   protected boolean match(Query query, Object obj) throws Exception {
      BaseMatcher matcher = createMatcher();

      final int[] matchCount = new int[1];

      matcher.registerFilter(query, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection, boolean isMatching) {
            if (isMatching) {
               matchCount[0]++;
            }
         }
      });

      matcher.match(obj);
      return matchCount[0] == 1;
   }

   @Test
   public void test1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";   //todo should we be able to use short names too?
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void test2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'George'";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void test3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.address.postCode = 'SW12345'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void test4() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.address.postCode = 'NW045'";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void test5() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number = '004012345'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void test6() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number = '11111'";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void test7() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name > 'G'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void test8() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name < 'G'";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void test9() throws Exception {
      String queryString1 = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";
      String queryString2 = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number = '004012345'";

      BaseMatcher matcher = createMatcher();

      final int[] matchCount1 = new int[1];
      matcher.registerFilter(queryString1, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection, boolean isMatching) {
            if (isMatching) {
               matchCount1[0]++;
            }
         }
      });

      final int[] matchCount2 = new int[1];
      matcher.registerFilter(queryString2, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection, boolean isMatching) {
            if (isMatching) {
               matchCount2[0]++;
            }
         }
      });

      matcher.match(createPerson());

      assertEquals(1, matchCount1[0]);
      assertEquals(1, matchCount2[0]);
   }

   @Test
   public void test10() throws Exception {
      BaseMatcher matcher = createMatcher();
      QueryFactory qf = matcher.getQueryFactory();
      Query q = qf.from(Person.class)
            .having("phoneNumbers.number")
            .eq("004012345").toBuilder().build();
      assertTrue(match(q, createPerson()));
   }

   @Test
   public void test11() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      BaseMatcher matcher = createMatcher();
      Object person = createPerson();

      final int[] matchCount = new int[1];
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection, boolean isMatching) {
            if (isMatching) {
               matchCount[0]++;
            }
         }
      });

      // create a sub-matcher with a single filter
      final int[] matchCount2 = new int[1];
      Matcher subMatcher = matcher.getSingleFilterMatcher(filterSubscription, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection, boolean isMatching) {
            if (isMatching) {
               matchCount2[0]++;
            }
         }
      });

      matcher.match(person);
      assertEquals(1, matchCount[0]);
      assertEquals(0, matchCount2[0]);

      subMatcher.match(person);
      assertEquals(1, matchCount[0]);
      assertEquals(1, matchCount2[0]);
   }

   @Test
   public void test12() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";   //todo should we be able to use short names too?

      BaseMatcher matcher = createMatcher();
      Object person = createPerson();

      final int matchCount[] = new int[1];
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection, boolean isMatching) {
            if (isMatching) {
               matchCount[0]++;
            }
         }
      });

      matcher.match(person);

      assertEquals(1, matchCount[0]);

      matcher.unregisterFilter(filterSubscription);
      matcher.match(person);

      // check that unregistration really took effect
      assertEquals(1, matchCount[0]);
   }
}
