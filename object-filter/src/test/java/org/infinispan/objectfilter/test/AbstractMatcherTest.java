package org.infinispan.objectfilter.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.objectfilter.test.model.Address;
import org.infinispan.objectfilter.test.model.Person;
import org.infinispan.objectfilter.test.model.PhoneNumber;
import org.infinispan.query.dsl.ProjectionConstants;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class AbstractMatcherTest {

   @Rule
   public ExpectedException expectedException = ExpectedException.none();

   protected abstract QueryFactory createQueryFactory();

   protected Object createPerson1() throws Exception {
      Person person = new Person();
      person.setId(1);
      person.setName("John");
      person.setSurname("Batman");
      person.setAge(40);
      person.setGender(Person.Gender.MALE);

      Address address = new Address();
      address.setStreet("Old Street");
      address.setPostCode("SW12345");
      person.setAddress(address);

      PhoneNumber phoneNumber1 = new PhoneNumber();
      phoneNumber1.setNumber("0040888888");
      PhoneNumber phoneNumber2 = new PhoneNumber();
      phoneNumber2.setNumber("004012345");
      person.setPhoneNumbers(Arrays.asList(phoneNumber1, phoneNumber2));
      return person;
   }

   protected Object createPerson2() throws Exception {
      Person person = new Person();
      person.setId(2);
      person.setName("Cat");
      person.setSurname("Woman");
      person.setAge(27);
      person.setGender(Person.Gender.FEMALE);
      return person;
   }

   protected abstract Matcher createMatcher();

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
   public void shouldRaiseExceptionDueToUnknownAlias() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028502");

      String queryString = "from org.infinispan.objectfilter.test.model.Person person where x.name = 'John'";
      match(queryString, createPerson1());
   }

   @Test
   public void shouldRaiseNoSuchPropertyException() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028501");

      String queryString = "from org.infinispan.objectfilter.test.model.Person person where person.name.blah = 'John'";
      match(queryString, createPerson1());
   }

   @Test
   public void shouldRaisePredicatesOnEntityAliasNotAllowedException1() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028519");

      String queryString = "from org.infinispan.objectfilter.test.model.Person name where name = 'John'";
      match(queryString, createPerson1());
   }

   @Test
   public void shouldRaisePredicatesOnEntityAliasNotAllowedException2() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028519");

      String queryString = "from org.infinispan.objectfilter.test.model.Person name where name is not null";
      match(queryString, createPerson1());
   }

   @Test
   public void testIntervalOverlap1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age <= 50 and age <= 40";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIntervalOverlap2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age <= 50 and age = 40";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIntervalOverlap3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age > 50 and age = 40";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   //todo this triggers a bug in hql parser (https://hibernate.atlassian.net/browse/HQLPARSER-44): NPE in SingleEntityQueryRendererDelegate.addComparisonPredicate due to null property path.
   public void testNoOpFilter1() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028524");

      String queryString = "from org.infinispan.objectfilter.test.model.Person where 4 = 4";
      match(queryString, createPerson1());
   }

   @Test
   @Ignore
   //todo this triggers a bug in hql parser (https://hibernate.atlassian.net/browse/HQLPARSER-44): second name token is mistakenly recognized as a string constant instead of property reference. this should trigger a parsing error.
   public void testNoOpFilter2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where name = name";  // this should match ALL
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testNoOpFilter3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person";  // this should match ALL
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testNoOpFilter4() throws Exception {
      String queryString = "select name from org.infinispan.objectfilter.test.model.Person";  // this should match ALL
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testEnum() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.gender = 'MALE'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testMissingProperty1() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028501");

      String queryString = "from org.infinispan.objectfilter.test.model.Person where missingProp is null";
      match(queryString, createPerson1());
   }

   @Test
   public void testMissingProperty2() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028501");

      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.missingProp is null";
      match(queryString, createPerson1());
   }

   @Test
   public void testIsNull1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.surname is null";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testIsNull2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.license is null";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIsNotNull1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.surname is not null";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIsNotNull2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where not(p.surname is null)";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testCollectionIsNull1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where phoneNumbers is null";
      assertTrue(match(queryString, createPerson2()));
   }

   @Test
   public void testCollectionIsNull2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where phoneNumbers is null";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testCollectionIsNotNull1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where phoneNumbers is not null";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testCollectionIsNotNull2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where phoneNumbers is not null";
      assertFalse(match(queryString, createPerson2()));
   }

   @Test
   public void testSimpleAttribute3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'George'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute4() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where not(p.name != 'George')";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttribute5() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name != 'George'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testNestedAttribute1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.address.postCode = 'SW12345'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testNestedAttribute2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.address.postCode = 'NW045'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testNestedRepeatedAttribute1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number = '004012345'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testNestedRepeatedAttribute2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number = '11111'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testNestedRepeatedAttribute3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number != '11111'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttributeInterval1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name > 'G'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testSimpleAttributeInterval2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name < 'G'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testFilterInterference() throws Exception {
      Matcher matcher = createMatcher();

      int[] matchCount = {0, 0};
      String queryString1 = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";
      matcher.registerFilter(queryString1, (userContext, eventType, instance, projection, sortProjection) -> matchCount[0]++);

      String queryString2 = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number = '004012345'";
      matcher.registerFilter(queryString2, (userContext, eventType, instance, projection, sortProjection) -> matchCount[1]++);

      matcher.match(null, null, createPerson1());

      // assert that only one of the filters matched and the callback of the other was not invoked
      assertEquals(1, matchCount[0]);
      assertEquals(1, matchCount[1]);
   }

   @Test
   public void testOrderBy() throws Exception {
      Matcher matcher = createMatcher();

      List<Comparable[]> sortProjections = new ArrayList<>();

      String queryString1 = "from org.infinispan.objectfilter.test.model.Person p where p.age > 18 order by p.name, p.surname";
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
   public void testDSL() throws Exception {
      QueryFactory qf = createQueryFactory();
      Query q = qf.from(Person.class)
            .having("phoneNumbers.number").eq("004012345").build();
      assertTrue(match(q, createPerson1()));
   }

   @Test
   public void testObjectFilterWithExistingSubscription() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      int[] matchCount = {0};
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> matchCount[0]++);

      ObjectFilter objectFilter = matcher.getObjectFilter(filterSubscription);

      matcher.match(null, null, person);
      assertEquals(1, matchCount[0]);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertTrue(result.getInstance() == person);

      assertEquals(1, matchCount[0]); // check that the object filter did not also mistakenly trigger a match in the parent matcher
   }

   @Test
   public void testObjectFilterWithQL() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

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

      QueryFactory qf = createQueryFactory();

      // use the same '< 1000' predicate on two different attributes to demonstrate they do not interfere (see ISPN-4654)
      Query q = qf.from(Person.class)
            .having("id").lt(1000)
            .and()
            .having("age").lt(1000)
            .build();

      ObjectFilter objectFilter = matcher.getObjectFilter(q);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertTrue(result.getInstance() == person);
   }

   @Test
   public void testObjectFilterWithDSLSamePredicate2() throws Exception {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      QueryFactory qf = createQueryFactory();

      // use the same "like 'Jo%'" predicate (in positive and negative form) on the same attribute to demonstrate they do not interfere (see ISPN-4654)
      Query q = qf.from(Person.class)
            .having("name").like("Jo%")
            .and(qf.not().having("name").like("Jo%").or().having("id").lt(1000))
            .build();

      ObjectFilter objectFilter = matcher.getObjectFilter(q);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertTrue(result.getInstance() == person);
   }

   @Test
   public void testMatcherAndObjectFilterWithDSL() throws Exception {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      QueryFactory qf = createQueryFactory();
      Query q = qf.from(Person.class)
            .having("name").eq("John").build();

      boolean b[] = new boolean[1];
      FilterSubscription filterSubscription = matcher.registerFilter(q, (userContext, eventType, instance, projection, sortProjection) -> b[0] = true);

      ObjectFilter objectFilter = matcher.getObjectFilter(filterSubscription);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertTrue(result.getInstance() == person);

      matcher.match(null, null, person);
      assertTrue(b[0]);
   }


   @Test
   public void testObjectFilterWithDSL() throws Exception {
      Matcher matcher = createMatcher();
      Object person = createPerson1();

      QueryFactory qf = createQueryFactory();
      Query q = qf.from(Person.class)
            .having("name").eq("John").build();

      ObjectFilter objectFilter = matcher.getObjectFilter(q);

      ObjectFilter.FilterResult result = objectFilter.filter(person);
      assertNotNull(result);
      assertTrue(result.getInstance() == person);
   }

   @Test
   public void testUnregistration() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      int matchCount[] = {0};
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> matchCount[0]++);

      matcher.match(null, null, person);

      assertEquals(1, matchCount[0]);

      matcher.unregisterFilter(filterSubscription);
      matcher.match(null, null, person);

      // check that unregistration really took effect
      assertEquals(1, matchCount[0]);
   }

   @Test
   public void testAnd1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.address.postCode = 'SW12345' and p.name = 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testAnd2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age > 10 and age < 30";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testAnd3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age > 30 and name >= 'John'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testAnd4() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where surname = 'X' and age > 10";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testOr1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age < 30 or age > 10";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testOr2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where surname = 'X' or name like 'Joh%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testOr3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where (p.gender = 'MALE' or p.name = 'John' or p.gender = 'FEMALE') and p.surname = 'Batman'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike1() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number like '0040%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike2() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number like '999%'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testLike3() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name like 'Jo%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike4() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name like 'Joh_'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike5() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name like 'Joh_nna'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testLike6() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name like '_oh_'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike7() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name like '_oh_noes'";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testLike8() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name like '%hn%'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike9() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name like '%hn'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testLike10() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name like 'Jo%hn'";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testIn() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age between 22 and 42";
      assertTrue(match(queryString, createPerson1()));
   }

   @Test
   public void testNotIn() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age not between 22 and 42";
      assertFalse(match(queryString, createPerson1()));
   }

   @Test
   public void testProjections() throws Exception {
      String queryString = "select p.name, p.address.postCode from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      List<Object[]> result = new ArrayList<>();

      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> result.add(projection));

      assertNotNull(filterSubscription.getProjection());
      assertEquals(2, filterSubscription.getProjection().length);
      assertEquals("name", filterSubscription.getProjection()[0]);
      assertEquals("address.postCode", filterSubscription.getProjection()[1]);

      matcher.match(null, null, person);

      matcher.unregisterFilter(filterSubscription);

      assertEquals(1, result.size());
      assertEquals(2, result.get(0).length);
      assertEquals("John", result.get(0)[0]);
      assertEquals("SW12345", result.get(0)[1]);
   }

   @Test
   public void testDuplicateProjections() throws Exception {
      String queryString = "select p.name, p.name, p.address.postCode from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      final List<Object[]> result = new ArrayList<>();

      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> result.add(projection));

      assertNotNull(filterSubscription.getProjection());
      assertEquals(3, filterSubscription.getProjection().length);
      assertEquals("name", filterSubscription.getProjection()[0]);
      assertEquals("name", filterSubscription.getProjection()[1]);
      assertEquals("address.postCode", filterSubscription.getProjection()[2]);

      matcher.match(null, null, person);

      matcher.unregisterFilter(filterSubscription);

      assertEquals(1, result.size());
      assertEquals(3, result.get(0).length);
      assertEquals("John", result.get(0)[0]);
      assertEquals("John", result.get(0)[1]);
      assertEquals("SW12345", result.get(0)[2]);
   }

   @Test
   public void testProjectionOnRepeatedAttribute() throws Exception {
      String queryString = "select p.address.postCode, p.phoneNumbers.number from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      List<Object[]> result = new ArrayList<>();

      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> result.add(projection));

      assertNotNull(filterSubscription.getProjection());
      assertEquals(2, filterSubscription.getProjection().length);
      assertEquals("address.postCode", filterSubscription.getProjection()[0]);
      assertEquals("phoneNumbers.number", filterSubscription.getProjection()[1]);

      matcher.match(null, null, person);

      matcher.unregisterFilter(filterSubscription);

      assertEquals(1, result.size());
      assertEquals(2, result.get(0).length);
      assertEquals("SW12345", result.get(0)[0]);
      //todo [anistor] it is unclear what whe should expect here...
      assertEquals("0040888888", result.get(0)[1]);  //expect the first phone number
   }

   @Test
   public void testProjectionOnEmbeddedEntity() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028503");

      String queryString = "select p.phoneNumbers from org.infinispan.objectfilter.test.model.Person p";

      Matcher matcher = createMatcher();

      matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> {
      });
   }

   /**
    * Test that projections are properly computed even if the query is a tautology so no predicates will ever be
    * computed.
    */
   @Test
   public void testTautologyAndProjections() throws Exception {
      String queryString = "select name from org.infinispan.objectfilter.test.model.Person where age < 30 or age >= 30";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      List<Object[]> result = new ArrayList<>();
      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> result.add(projection));

      assertNotNull(filterSubscription.getProjection());
      assertEquals(1, filterSubscription.getProjection().length);
      assertEquals("name", filterSubscription.getProjection()[0]);

      matcher.match(null, null, person);

      matcher.unregisterFilter(filterSubscription);

      assertEquals(1, result.size());
      assertEquals(1, result.get(0).length);
      assertEquals("John", result.get(0)[0]);
   }

   @Test
   public void testProjectionOnValue() throws Exception {
      String queryString = "select " + ProjectionConstants.VALUE + " from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      final List<Object[]> result = new ArrayList<>();

      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, instance, eventType, projection, sortProjection) -> result.add(projection));

      assertNotNull(filterSubscription.getProjection());
      assertEquals(1, filterSubscription.getProjection().length);
      assertEquals(ProjectionConstants.VALUE, filterSubscription.getProjection()[0]);

      matcher.match(null, person, null);

      matcher.unregisterFilter(filterSubscription);

      assertTrue(person == result.get(0)[0]);
   }

   @Test
   public void testProjectionOnKey() throws Exception {
      String queryString = "select " + ProjectionConstants.KEY + " from org.infinispan.objectfilter.test.model.Person p where p.name = 'John'";

      Matcher matcher = createMatcher();
      Object person = createPerson1();

      final List<Object[]> result = new ArrayList<>();

      FilterSubscription filterSubscription = matcher.registerFilter(queryString, (userContext, instance, eventType, projection, sortProjection) -> result.add(projection));

      assertNotNull(filterSubscription.getProjection());
      assertEquals(1, filterSubscription.getProjection().length);
      assertEquals(ProjectionConstants.VALUE, filterSubscription.getProjection()[0]);

      matcher.match(null, person, null);

      matcher.unregisterFilter(filterSubscription);

      assertTrue(person == result.get(0)[0]);
   }

   @Test
   public void testDisallowGroupingAndAggregations() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("Filters cannot use grouping or aggregations");

      String queryString = "SELECT sum(p.age) " +
            "FROM org.infinispan.objectfilter.test.model.Person p " +
            "WHERE p.age <= 99 " +
            "GROUP BY p.name " +
            "HAVING COUNT(p.age) > 3";

      Matcher matcher = createMatcher();

      matcher.registerFilter(queryString, (userContext, eventType, instance, projection, sortProjection) -> {
      });
   }
}
