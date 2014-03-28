package org.infinispan.objectfilter.test;

import org.infinispan.objectfilter.ReflectionMatcher;
import org.infinispan.objectfilter.test.model.Address;
import org.infinispan.objectfilter.test.model.Person;
import org.infinispan.objectfilter.test.model.PhoneNumber;

import java.util.Arrays;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionMatcherTest extends AbstractMatcherTest {

   protected Person createPerson() throws Exception {
      Person person = new Person();
      person.setName("John");
      person.setSurname("Batman");

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

   protected ReflectionMatcher createMatcher() {
      return new ReflectionMatcher(null);
   }
}
