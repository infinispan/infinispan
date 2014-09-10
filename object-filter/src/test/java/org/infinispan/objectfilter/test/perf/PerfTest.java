package org.infinispan.objectfilter.test.perf;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.test.model.Address;
import org.infinispan.objectfilter.test.model.Person;
import org.infinispan.objectfilter.test.model.PhoneNumber;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Category(Profiling.class)
@Ignore public class PerfTest {

   private final int ITERATIONS = 10000000;

   private final int NUM_FILTERS = 1;

   @Test
   public void testLikeMatchPerf() throws Exception {
      long time = measureMatch("from org.infinispan.objectfilter.test.model.Person p where p.name like 'Jo%'");
      printTime("testLikeMatchPerf", time);
   }

   @Test
   public void testIsNullMatchPerf() throws Exception {
      long time = measureMatch("from org.infinispan.objectfilter.test.model.Person p where p.name is not null");
      printTime("testIsNullMatchPerf", time);
   }

   @Test
   public void testComplexMatchPerf() throws Exception {
      long time = measureMatch("from org.infinispan.objectfilter.test.model.Person p where p.surname = 'Batman' and p.age > 30 and p.name > 'A' and p.address.postCode = 'SW12345'");
      printTime("testComplexMatchPerf", time);
   }

   protected long measureMatch(String query) throws Exception {
      Matcher matcher = createMatcher();

      Object obj = createPerson1();

      final int[] matchCount = new int[1];

      for (int k = 0; k < NUM_FILTERS; k++) {
         matcher.registerFilter(query, new FilterCallback() {
            @Override
            public void onFilterResult(Object instance, Object[] projection, Comparable[] sortProjection) {
               matchCount[0]++;
            }
         });
      }

      long stime = System.nanoTime();
      for (int i = 0; i < ITERATIONS; i++) {
         matchCount[0] = 0;
         matcher.match(obj);
         assertEquals(NUM_FILTERS, matchCount[0]);
      }
      return System.nanoTime() - stime;
   }

   @Test
   public void testSimpleObjectFilterPerf() throws Exception {
      long time = measureFilter("from org.infinispan.objectfilter.test.model.Person p where p.name is not null");
      printTime("testSimpleObjectFilterPerf", time);
   }

   @Test
   public void testComplexObjectFilterPerf() throws Exception {
      long time = measureFilter("from org.infinispan.objectfilter.test.model.Person p where p.surname = 'Batman' and p.age > 30 and p.name > 'A' and p.address.postCode = 'SW12345'");
      printTime("testComplexObjectFilterPerf", time);
   }

   protected long measureFilter(String query) throws Exception {
      Matcher matcher = createMatcher();

      Object obj = createPerson1();

      ObjectFilter objectFilter = matcher.getObjectFilter(query);

      long stime = System.nanoTime();
      for (int i = 0; i < ITERATIONS; i++) {
         ObjectFilter.FilterResult result = objectFilter.filter(obj);
         assertNotNull(result);
      }
      return System.nanoTime() - stime;
   }

   protected void printTime(String text, long totalTime) {
      double iterationTime = totalTime / 1000;
      iterationTime /= ITERATIONS;
      System.out.println(getClass().getSimpleName() + "." + text + " " + iterationTime + "us");
   }

   protected Matcher createMatcher() throws Exception {
      return new ReflectionMatcher(null);
   }

   protected Object createPerson1() throws Exception {
      Person person = new Person();
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
}
