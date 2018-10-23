package org.infinispan.objectfilter.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.objectfilter.impl.RowMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.RowPropertyHelper;
import org.infinispan.objectfilter.test.model.Person;
import org.infinispan.query.dsl.Query;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public class RowMatcherTest {

   @Rule
   public ExpectedException expectedException = ExpectedException.none();

   private final FilterQueryFactory queryFactory = new FilterQueryFactory();

   private Object[] createPerson1() {
      // id, name, surname, age, gender
      return new Object[]{1, "John", "Batman", 40, Person.Gender.MALE, null};
   }

   private Object[] createPerson2() {
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

   protected boolean match(String queryString, Object obj) {
      Matcher matcher = createMatcher();

      int[] matchCount = {0};
      matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> matchCount[0]++);

      matcher.match(null, null, obj);
      return matchCount[0] == 1;
   }

   protected boolean match(Query query, Object obj) {
      Matcher matcher = createMatcher();

      int[] matchCount = {0};
      matcher.registerFilter(query, (userContext, eventType, instance, projection, sortProjection) -> matchCount[0]++);

      matcher.match(null, null, obj);
      return matchCount[0] == 1;
   }

   @Test
   public void shouldRaiseExceptionDueToUnknownAlias() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028502");

      String queryString = "from Row p where x.name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIntervalOverlap1() {
      String queryString = "from Row where age <= 50 and age <= 40";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIntervalOverlap2() {
      String queryString = "from Row where age <= 50 and age = 40";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIntervalOverlap3() {
      String queryString = "from Row where age > 50 and age = 40";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testNoOpFilter3() {
      String queryString = "from Row";  // this should match ALL
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute1() {
      String queryString = "from Row where name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute2() {
      String queryString = "from Row p where p.name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testEnum() {
      String queryString = "from Row p where p.gender = 'MALE'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testMissingProperty1() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028501");

      String queryString = "from Row where missingProp is null";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testMissingProperty2() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028501");

      String queryString = "from Row p where p.missingProp is null";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testIsNull1() {
      String queryString = "from Row p where p.surname is null";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testIsNull2() {
      String queryString = "from Row p where p.license is null";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIsNotNull() {
      String queryString = "from Row p where p.surname is not null";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute3() {
      String queryString = "from Row p where p.name = 'George'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute4() {
      String queryString = "from Row p where not(p.name != 'George')";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute5() {
      String queryString = "from Row p where p.name != 'George'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttributeInterval1() {
      String queryString = "from Row p where p.name > 'G'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttributeInterval2() {
      String queryString = "from Row p where p.name < 'G'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testFilterInterference() {
      Matcher matcher = createMatcher();

      int[] matchCount = {0, 0};
      String queryString1 = "from Row p where p.name = 'John'";
      matcher.registerFilter(queryString1, (userContext, eventType, instance, projection, sortProjection) -> matchCount[0]++);

      String queryString2 = "from Row p where p.age = 40";
      matcher.registerFilter(queryString2, (userContext, eventType, instance, projection, sortProjection) -> matchCount[1]++);

      matcher.match(null, null, createPerson1());

      // assert that only one of the filters matched and the callback of the other was not invoked
      assertEquals(1, matchCount[0]);
      assertEquals(1, matchCount[1]);
   }

   @Test
   public void testOrderBy() {
      Matcher matcher = createMatcher();

      List<Comparable[]> sortProjections = new ArrayList<>();

      String queryString1 = "from Row p where p.age > 18 order by p.name, p.surname";
      FilterSubscription filterSubscription = matcher.registerFilter(queryString1, (userContext, eventType, instance, projection, sortProjection) -> sortProjections.add(sortProjection));

      matcher.match(null, null, createPerson1());
      matcher.match(null, null, createPerson2());

      assertEquals(2, sortProjections.size());

      sortProjections.sort(filterSubscription.getComparator());

      assertEquals("Cat", sortProjections.get(0)[0]);
      assertEquals("Woman", sortProjections.get(0)[1]);
      assertEquals("John", sortProjections.get(1)[0]);
      assertEquals("Batman", sortProjections.get(1)[1]);
   }

   @Test
   public void testDSL() {
      Query q = queryFactory.from(Person.class)
            .having("name").eq("John").build();
      assertTrue(match(q, createPerson1()));
   }

   @Test
   public void testObjectFilterWithExistingSubscription() {
      String queryString = "from Row p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      int[] matchCount = {0};
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> matchCount[0]++);

      ObjectFilter objectFilter = matcher.getObjectFilter(filterSubscription);

      matcher.match(null, null, person);
      assertEquals(1, matchCount[0]);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertSame(person, result.getInstance());

      assertEquals(1, matchCount[0]); // check that the object filter did not also mistakenly trigger a match in the parent matcher
   }

   @Test
   public void testObjectFilterWithIckle() {
      String queryString = "from Row p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      ObjectFilter objectFilter = matcher.getObjectFilter(queryString);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertSame(person, result.getInstance());
   }

   @Test
   public void testObjectFilterWithDSLSamePredicate1() {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      // use the same '< 1000' predicate on two different attributes to demonstrate they do not interfere (see ISPN-4654)
      Query q = queryFactory.from(Person.class)
            .having("id").lt(1000)
            .and()
            .having("age").lt(1000)
            .build();

      ObjectFilter objectFilter = matcher.getObjectFilter(q);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertSame(person, result.getInstance());
   }

   @Test
   public void testObjectFilterWithDSLSamePredicate2() {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      // use the same "like 'Jo%'" predicate (in positive and negative form) on the same attribute to demonstrate they do not interfere (see ISPN-4654)
      Query q = queryFactory.from(Person.class)
            .having("name").like("Jo%")
            .and(queryFactory.not().having("name").like("Jo%").or().having("id").lt(1000))
            .build();

      ObjectFilter objectFilter = matcher.getObjectFilter(q);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertSame(person, result.getInstance());
   }

   @Test
   public void testMatcherAndObjectFilterWithDSL() {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      Query q = queryFactory.from(Person.class)
            .having("name").eq("John").build();

      boolean b[] = {false};
      FilterSubscription filterSubscription = matcher.registerFilter(q, (userContext, eventType, instance, projection, sortProjection) -> b[0] = true);

      ObjectFilter objectFilter = matcher.getObjectFilter(filterSubscription);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertSame(person, result.getInstance());

      matcher.match(null, null, person);
      assertTrue(b[0]);
   }

   @Test
   public void testObjectFilterWithDSL() {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      Query q = queryFactory.from(Person.class)
            .having("name").eq("John").build();

      ObjectFilter objectFilter = matcher.getObjectFilter(q);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertSame(person, result.getInstance());
   }

   @Test
   public void testUnregistration() {
      String queryString = "from Row p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      int matchCount[] = new int[1];
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> matchCount[0]++);

      matcher.match(null, null, person);

      assertEquals(1, matchCount[0]);

      matcher.unregisterFilter(filterSubscription);
      matcher.match(null, null, person);

      // check that unregistration really took effect
      assertEquals(1, matchCount[0]);
   }

   @Test
   public void testAnd1() {
      String queryString = "from Row p where p.age < 44 and p.name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testAnd2() {
      String queryString = "from Row where age > 10 and age < 30";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testAnd3() {
      String queryString = "from Row where age > 30 and name >= 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testAnd4() {
      String queryString = "from Row where surname = 'X' and age > 10";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testOr1() {
      String queryString = "from Row where age < 30 or age > 10";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testOr2() {
      String queryString = "from Row where surname = 'X' or name like 'Joh%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testOr3() {
      String queryString = "from Row p where (p.gender = 'MALE' or p.name = 'John' or p.gender = 'FEMALE') and p.surname = 'Batman'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike1() {
      String queryString = "from Row p where p.name like 'Jo%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike2() {
      String queryString = "from Row p where p.name like 'Ja%'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testLike3() {
      String queryString = "from Row p where p.name like 'Jo%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike4() {
      String queryString = "from Row p where p.name like 'Joh_'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike5() {
      String queryString = "from Row p where p.name like 'Joh_nna'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testLike6() {
      String queryString = "from Row p where p.name like '_oh_'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike7() {
      String queryString = "from Row p where p.name like '_oh_noes'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testLike8() {
      String queryString = "from Row p where p.name like '%hn%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike9() {
      String queryString = "from Row p where p.name like '%hn'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike10() {
      String queryString = "from Row p where p.name like 'Jo%hn'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIn() {
      String queryString = "from Row where age between 22 and 42";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testNotIn() {
      String queryString = "from Row where age not between 22 and 42";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testProjections() {
      String queryString = "select p.name, p.age from Row p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      List<Object[]> result = new ArrayList<>();

      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> result.add(projection));

      assertNotNull(filterSubscription.getProjection());
      assertEquals(2, filterSubscription.getProjection().length);
      assertEquals("name", filterSubscription.getProjection()[0]);
      assertEquals("age", filterSubscription.getProjection()[1]);

      matcher.match(null, null, person);

      matcher.unregisterFilter(filterSubscription);

      assertEquals(1, result.size());
      assertEquals(2, result.get(0).length);
      assertEquals("John", result.get(0)[0]);
      assertEquals(40, result.get(0)[1]);
   }

   @Test
   public void testDuplicateProjections() {
      String queryString = "select p.name, p.name, p.age from Row p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      List<Object[]> result = new ArrayList<>();
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> result.add(projection));

      assertNotNull(filterSubscription.getProjection());
      assertEquals(3, filterSubscription.getProjection().length);
      assertEquals("name", filterSubscription.getProjection()[0]);
      assertEquals("name", filterSubscription.getProjection()[1]);
      assertEquals("age", filterSubscription.getProjection()[2]);

      matcher.match(null, null, person);

      matcher.unregisterFilter(filterSubscription);

      assertEquals(1, result.size());
      assertEquals(3, result.get(0).length);
      assertEquals("John", result.get(0)[0]);
      assertEquals("John", result.get(0)[1]);
      assertEquals(40, result.get(0)[2]);
   }

   @Test
   public void testDisallowGroupingAndAggregations() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("Filters cannot use grouping or aggregations");

      String queryString = "SELECT sum(p.age) " +
            "from Row p " +
            "WHERE p.age <= 99 " +
            "GROUP BY p.name " +
            "HAVING count(p.age) > 3";

      Matcher matcher = createMatcher();

      matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> {
      });
   }
}
