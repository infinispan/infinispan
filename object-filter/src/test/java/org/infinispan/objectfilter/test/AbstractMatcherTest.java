package org.infinispan.objectfilter.test;

import org.hibernate.hql.ParsingException;
import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.test.model.Person;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class AbstractMatcherTest {

   @Rule
   public ExpectedException expectedException = ExpectedException.none();

   protected abstract Object createPerson() throws Exception;

   protected abstract Matcher createMatcher();

   protected boolean match(String queryString, Object obj) throws Exception {
      Matcher matcher = createMatcher();

      final int[] matchCount = new int[1];

      matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection) {
            matchCount[0]++;
         }
      });

      matcher.match(obj);
      return matchCount[0] == 1;
   }

   protected boolean match(Query query, Object obj) throws Exception {
      Matcher matcher = createMatcher();

      final int[] matchCount = new int[1];

      matcher.registerFilter(query, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection) {
            matchCount[0]++;
         }
      });

      matcher.match(obj);
      return matchCount[0] == 1;
   }

   @Test
   public void shouldRaiseExceptionDueToUnknownAlias() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000404");

      String queryString = "from org.infinispan.objectfilter.test.model.Person person where x.name = 'John'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   @Ignore
   //todo this triggers a bug in hql parser: NPE in SingleEntityQueryRendererDelegate.addComparisonPredicate due to null property path
   public void testNoOpFilter1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where 4 = 4";  // this should match ALL
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   @Ignore
   //todo this triggers a bug in hql parser: second name token is mistakenly recognized as a string constant instead of property reference. this should trigger a parsing error
   public void testNoOpFilter2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where name = name";  // this should match ALL
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testNoOpFilter3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person";  // this should match ALL
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testSimpleAttribute1() throws Exception {
      //todo [anistor] should we be able to use short class names too? how would we guess the full name then?
      String queryString = "from org.infinispan.objectfilter.test.model.Person where name = 'John'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testSimpleAttribute2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testMissingProperty1() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000402");

      String queryString = "from org.infinispan.objectfilter.test.model.Person where missingProp is null";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void testMissingProperty2() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000402");

      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.missingProp is null";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void testIsNull1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.surname is null";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void testIsNull2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.license is null";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testIsNotNull() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.surname is not null";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testSimpleAttribute3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'George'";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void testSimpleAttribute4() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where not(p.name != 'George')";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void testSimpleAttribute5() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name != 'George'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testNestedAttribute1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.address.postCode = 'SW12345'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testNestedAttribute2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.address.postCode = 'NW045'";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void testNestedRepeatedAttribute1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number = '004012345'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testNestedRepeatedAttribute2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number = '11111'";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void testSimpleAttributeInterval1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name > 'G'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testSimpleAttributeInterval2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name < 'G'";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void testFilterInterference() throws Exception {
      Matcher matcher = createMatcher();

      final int[] matchCount = new int[2];
      String queryString1 = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";
      matcher.registerFilter(queryString1, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection) {
            matchCount[0]++;
         }
      });

      String queryString2 = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number = '004012345'";
      matcher.registerFilter(queryString2, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projectiong) {
            matchCount[1]++;
         }
      });

      matcher.match(createPerson());

      // assert that only one of the filters matched and the callback of the other was not invoked
      assertEquals(1, matchCount[0]);
      assertEquals(1, matchCount[1]);
   }

   @Test
   public void testDSL() throws Exception {
      Matcher matcher = createMatcher();
      QueryFactory qf = matcher.getQueryFactory();
      Query q = qf.from(Person.class)
            .having("phoneNumbers.number").eq("004012345").toBuilder().build();
      assertTrue(match(q, createPerson()));
   }

   @Test
   public void testObjectFilterWithExistingSubscription() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson();

      final int[] matchCount = new int[1];
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection) {
            matchCount[0]++;
         }
      });

      ObjectFilter objectFilter = matcher.getObjectFilter(filterSubscription);

      matcher.match(person);
      assertEquals(1, matchCount[0]);

      Object result = objectFilter.filter(person);
      assertTrue(result == person);

      assertEquals(1, matchCount[0]); // check that the object filter did not also mistakenly trigger a match in the parent matcher
   }

   @Test
   public void testObjectFilterWithJPA() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson();

      ObjectFilter objectFilter = matcher.getObjectFilter(queryString);

      matcher.match(person);

      Object result = objectFilter.filter(person);
      assertTrue(result == person);
   }

   @Test
   public void testObjectFilterWithDSL() throws Exception {
      Matcher matcher = createMatcher();
      Object person = createPerson();

      QueryFactory qf = matcher.getQueryFactory();
      Query q = qf.from(Person.class)
            .having("name").eq("John").toBuilder().build();

      ObjectFilter objectFilter = matcher.getObjectFilter(q);

      matcher.match(person);

      Object result = objectFilter.filter(person);
      assertTrue(result == person);
   }

   @Test
   public void testUnregistration() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson();

      final int matchCount[] = new int[1];
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection) {
            matchCount[0]++;
         }
      });

      matcher.match(person);

      assertEquals(1, matchCount[0]);

      matcher.unregisterFilter(filterSubscription);
      matcher.match(person);

      // check that unregistration really took effect
      assertEquals(1, matchCount[0]);
   }

   @Test
   public void testAnd1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.address.postCode = 'SW12345' and p.name = 'John'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testAnd2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age > 10 and age < 30";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void testAnd3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age > 30 and name >= 'John'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testAnd4() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where surname = 'X' and age > 10";
      assertFalse(match(queryString, createPerson()));
   }

   @Test
   public void testOr1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age < 30 or age > 10";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testOr2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where surname = 'X' or name like 'Joh%'";
      assertTrue(match(queryString, createPerson()));
   }

   @Test
   public void testProjections() throws Exception {
      String queryString = "select p.name, p.address.postCode from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson();

      final List<Object[]> result = new ArrayList<Object[]>();

      FilterSubscription filterSubscription = matcher.registerFilter(queryString, new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection) {
            result.add(projection);
         }
      });

      assertNotNull(filterSubscription.getProjection());
      assertEquals(2, filterSubscription.getProjection().length);
      assertEquals("name", filterSubscription.getProjection()[0]);
      assertEquals("address.postCode", filterSubscription.getProjection()[1]);

      matcher.match(person);

      matcher.unregisterFilter(filterSubscription);

      assertEquals(1, result.size());
      assertEquals(2, result.get(0).length);
      assertEquals("John", result.get(0)[0]);
      assertEquals("SW12345", result.get(0)[1]);
   }
}
