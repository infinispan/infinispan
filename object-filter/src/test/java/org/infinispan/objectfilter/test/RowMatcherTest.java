package org.infinispan.objectfilter.test;

import org.hibernate.hql.ParsingException;
import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.RowMatcher;
import org.infinispan.objectfilter.impl.hql.RowPropertyHelper;
import org.infinispan.objectfilter.test.model.Person;
import org.infinispan.query.dsl.Query;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public class RowMatcherTest {

   private final FilterQueryFactory queryFactory = new FilterQueryFactory();

   @Rule
   public ExpectedException expectedException = ExpectedException.none();

   private Object[] createPerson1() throws Exception {
      // id, name, surname, age, gender
      return new Object[]{1, "John", "Batman", 40, Person.Gender.MALE, null};
   }

   private Object[] createPerson2() throws Exception {
      // id, name, surname, age, gender
      return new Object[]{2, "Cat", "Woman", 27, Person.Gender.FEMALE, null};
   }

   private RowMatcher createMatcher() {
      RowPropertyHelper.ColumnMetadata[] columns = new RowPropertyHelper.ColumnMetadata[]{
            new RowPropertyHelper.ColumnMetadata(0, "id", Integer.class),
            new RowPropertyHelper.ColumnMetadata(1, "name", String.class),
            new RowPropertyHelper.ColumnMetadata(2, "surname", String.class),
            new RowPropertyHelper.ColumnMetadata(3, "age", Integer.class),
            new RowPropertyHelper.ColumnMetadata(4, "gender", Person.Gender.class),
            new RowPropertyHelper.ColumnMetadata(5, "license", String.class),
      };

      return new RowMatcher(columns);
   }

   protected boolean match(String queryString, Object obj) throws Exception {
      Matcher matcher = createMatcher();

      final int[] matchCount = new int[1];

      matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
            matchCount[0]++;
         }
      });

      matcher.match(null, obj, null);
      return matchCount[0] == 1;
   }

   protected boolean match(Query query, Object obj) throws Exception {
      Matcher matcher = createMatcher();

      final int[] matchCount = new int[1];

      matcher.registerFilter(query, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
            matchCount[0]++;
         }
      });

      matcher.match(null, obj, null);
      return matchCount[0] == 1;
   }

   @Test
   public void shouldRaiseExceptionDueToUnknownAlias() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000404");

      String queryString = "from Row p where x.name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIntervalOverlap1() throws Exception {
      String queryString = "from Row where age <= 50 and age <= 40";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIntervalOverlap2() throws Exception {
      String queryString = "from Row where age <= 50 and age = 40";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIntervalOverlap3() throws Exception {
      String queryString = "from Row where age > 50 and age = 40";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testNoOpFilter3() throws Exception {
      String queryString = "from Row";  // this should match ALL
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute1() throws Exception {
      String queryString = "from Row where name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute2() throws Exception {
      String queryString = "from Row p where p.name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testEnum() throws Exception {
      String queryString = "from Row p where p.gender = 'MALE'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testMissingProperty1() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000402");

      String queryString = "from Row where missingProp is null";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testMissingProperty2() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000402");

      String queryString = "from Row p where p.missingProp is null";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testIsNull1() throws Exception {
      String queryString = "from Row p where p.surname is null";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testIsNull2() throws Exception {
      String queryString = "from Row p where p.license is null";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIsNotNull() throws Exception {
      String queryString = "from Row p where p.surname is not null";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute3() throws Exception {
      String queryString = "from Row p where p.name = 'George'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute4() throws Exception {
      String queryString = "from Row p where not(p.name != 'George')";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute5() throws Exception {
      String queryString = "from Row p where p.name != 'George'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttributeInterval1() throws Exception {
      String queryString = "from Row p where p.name > 'G'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttributeInterval2() throws Exception {
      String queryString = "from Row p where p.name < 'G'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testFilterInterference() throws Exception {
      Matcher matcher = createMatcher();

      final int[] matchCount = new int[2];
      String queryString1 = "from Row p where p.name = 'John'";
      matcher.registerFilter(queryString1, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
            matchCount[0]++;
         }
      });

      String queryString2 = "from Row p where p.age = 40";
      matcher.registerFilter(queryString2, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
            matchCount[1]++;
         }
      });

      matcher.match(null, createPerson1(), null);

      // assert that only one of the filters matched and the callback of the other was not invoked
      assertEquals(1, matchCount[0]);
      assertEquals(1, matchCount[1]);
   }

   @Test
   public void testOrderBy() throws Exception {
      Matcher matcher = createMatcher();

      final List<Comparable[]> sortProjections = new ArrayList<Comparable[]>();

      String queryString1 = "from Row p where p.age > 18 order by p.name, p.surname";
      FilterSubscription filterSubscription = matcher.registerFilter(queryString1, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
            sortProjections.add(sortProjection);
         }
      });

      matcher.match(null, createPerson1(), null);
      matcher.match(null, createPerson2(), null);

      assertEquals(2, sortProjections.size());

      Collections.sort(sortProjections, filterSubscription.getComparator());

      assertEquals("Cat", sortProjections.get(0)[0]);
      assertEquals("Woman", sortProjections.get(0)[1]);
      assertEquals("John", sortProjections.get(1)[0]);
      assertEquals("Batman", sortProjections.get(1)[1]);
   }

   @Test
   public void testDSL() throws Exception {
      Query q = queryFactory.from(Person.class)
            .having("name").eq("John").toBuilder().build();
      assertTrue(match(q, createPerson1()));
   }

   @Test
   public void testObjectFilterWithExistingSubscription() throws Exception {
      String queryString = "from Row p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      final int[] matchCount = new int[1];
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
            matchCount[0]++;
         }
      });

      ObjectFilter objectFilter = matcher.getObjectFilter(filterSubscription);

      matcher.match(null, person, null);
      assertEquals(1, matchCount[0]);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertTrue(result.getInstance() == person);

      assertEquals(1, matchCount[0]); // check that the object filter did not also mistakenly trigger a match in the parent matcher
   }

   @Test
   public void testObjectFilterWithJPA() throws Exception {
      String queryString = "from Row p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      ObjectFilter objectFilter = matcher.getObjectFilter(queryString);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertTrue(result.getInstance() == person);
   }

   @Test
   public void testObjectFilterWithDSLSamePredicate1() throws Exception {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      // use the same '< 1000' predicate on two different attributes to demonstrate they do not interfere (see ISPN-4654)
      Query q = queryFactory.from(Person.class)
            .having("id").lt(1000)
            .and()
            .having("age").lt(1000)
            .toBuilder().build();

      ObjectFilter objectFilter = matcher.getObjectFilter(q);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertTrue(result.getInstance() == person);
   }

   @Test
   public void testObjectFilterWithDSLSamePredicate2() throws Exception {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      // use the same "like 'Jo%'" predicate (in positive and negative form) on the same attribute to demonstrate they do not interfere (see ISPN-4654)
      Query q = queryFactory.from(Person.class)
            .having("name").like("Jo%")
            .and(queryFactory.not().having("name").like("Jo%").or().having("id").lt(1000))
            .toBuilder().build();

      ObjectFilter objectFilter = matcher.getObjectFilter(q);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertTrue(result.getInstance() == person);
   }

   @Test
   public void testMatcherAndObjectFilterWithDSL() throws Exception {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      Query q = queryFactory.from(Person.class)
            .having("name").eq("John").toBuilder().build();

      final boolean b[] = new boolean[1];

      FilterSubscription filterSubscription = matcher.registerFilter(q, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
            b[0] = true;
         }
      });

      ObjectFilter objectFilter = matcher.getObjectFilter(filterSubscription);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertTrue(result.getInstance() == person);

      matcher.match(null, person, null);
      assertTrue(b[0]);
   }


   @Test
   public void testObjectFilterWithDSL() throws Exception {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      Query q = queryFactory.from(Person.class)
            .having("name").eq("John").toBuilder().build();

      ObjectFilter objectFilter = matcher.getObjectFilter(q);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertTrue(result.getInstance() == person);
   }

   @Test
   public void testUnregistration() throws Exception {
      String queryString = "from Row p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      final int matchCount[] = new int[1];
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
            matchCount[0]++;
         }
      });

      matcher.match(null, person, null);

      assertEquals(1, matchCount[0]);

      matcher.unregisterFilter(filterSubscription);
      matcher.match(null, person, null);

      // check that unregistration really took effect
      assertEquals(1, matchCount[0]);
   }

   @Test
   public void testAnd1() throws Exception {
      String queryString = "from Row p where p.age < 44 and p.name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testAnd2() throws Exception {
      String queryString = "from Row where age > 10 and age < 30";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testAnd3() throws Exception {
      String queryString = "from Row where age > 30 and name >= 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testAnd4() throws Exception {
      String queryString = "from Row where surname = 'X' and age > 10";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testOr1() throws Exception {
      String queryString = "from Row where age < 30 or age > 10";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testOr2() throws Exception {
      String queryString = "from Row where surname = 'X' or name like 'Joh%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testOr3() throws Exception {
      String queryString = "from Row p where (p.gender = 'MALE' or p.name = 'John' or p.gender = 'FEMALE') and p.surname = 'Batman'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike1() throws Exception {
      String queryString = "from Row p where p.name like 'Jo%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike2() throws Exception {
      String queryString = "from Row p where p.name like 'Ja%'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testLike3() throws Exception {
      String queryString = "from Row p where p.name like 'Jo%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike4() throws Exception {
      String queryString = "from Row p where p.name like 'Joh_'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike5() throws Exception {
      String queryString = "from Row p where p.name like 'Joh_nna'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testLike6() throws Exception {
      String queryString = "from Row p where p.name like '_oh_'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike7() throws Exception {
      String queryString = "from Row p where p.name like '_oh_noes'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testLike8() throws Exception {
      String queryString = "from Row p where p.name like '%hn%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike9() throws Exception {
      String queryString = "from Row p where p.name like '%hn'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike10() throws Exception {
      String queryString = "from Row p where p.name like 'Jo%hn'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIn() throws Exception {
      String queryString = "from Row where age between 22 and 42";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testNotIn() throws Exception {
      String queryString = "from Row where age not between 22 and 42";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testProjections() throws Exception {
      String queryString = "select p.name, p.age from Row p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      final List<Object[]> result = new ArrayList<Object[]>();

      FilterSubscription filterSubscription = matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
            result.add(projection);
         }
      });

      assertNotNull(filterSubscription.getProjection());
      assertEquals(2, filterSubscription.getProjection().length);
      assertEquals("name", filterSubscription.getProjection()[0]);
      assertEquals("age", filterSubscription.getProjection()[1]);

      matcher.match(null, person, null);

      matcher.unregisterFilter(filterSubscription);

      assertEquals(1, result.size());
      assertEquals(2, result.get(0).length);
      assertEquals("John", result.get(0)[0]);
      assertEquals(40, result.get(0)[1]);
   }

   @Test
   public void testDuplicateProjections() throws Exception {
      String queryString = "select p.name, p.name, p.age from Row p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      final List<Object[]> result = new ArrayList<Object[]>();

      FilterSubscription filterSubscription = matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
            result.add(projection);
         }
      });

      assertNotNull(filterSubscription.getProjection());
      assertEquals(3, filterSubscription.getProjection().length);
      assertEquals("name", filterSubscription.getProjection()[0]);
      assertEquals("name", filterSubscription.getProjection()[1]);
      assertEquals("age", filterSubscription.getProjection()[2]);

      matcher.match(null, person, null);

      matcher.unregisterFilter(filterSubscription);

      assertEquals(1, result.size());
      assertEquals(3, result.get(0).length);
      assertEquals("John", result.get(0)[0]);
      assertEquals("John", result.get(0)[1]);
      assertEquals(40, result.get(0)[2]);
   }

   @Test
   public void testDisallowGroupingAndAggregations() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("Matcher and ObjectFilter do not allow grouping or aggregations");

      String queryString = "SELECT sum(p.age) " +
            "from Row p " +
            "WHERE p.age <= 99 " +
            "GROUP BY p.name " +
            "HAVING count(p.age) > 3";

      Matcher matcher = createMatcher();

      matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
         }
      });
   }
}
