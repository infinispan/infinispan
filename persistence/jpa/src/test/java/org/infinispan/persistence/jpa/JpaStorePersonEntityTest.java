package org.infinispan.persistence.jpa;

import java.util.HashSet;

import org.infinispan.persistence.jpa.entity.Address;
import org.infinispan.persistence.jpa.entity.Person;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.JpaStorePersonEntityTest")
public class JpaStorePersonEntityTest extends BaseJpaStoreTest {
   @Override
   protected Class<?> getEntityClass() {
      return Person.class;
   }

   @Override
   protected TestObject createTestObject(String key) {
      Address adr = new Address();
      adr.setCity("Brno");
      adr.setStreet("Purkynova 2855");
      adr.setZipCode(61200);
      
      Address secAdr1 = new Address();
      secAdr1.setCity("Brno - Kralovo Pole");
      secAdr1.setStreet("Purkynova 97");
      secAdr1.setZipCode(12345);
      
      Address secAdr2 = new Address();
      secAdr2.setCity("Kralovo Pole");
      secAdr2.setStreet("Purkynova 97a");
      secAdr2.setZipCode(54321);

      HashSet<Address> secAdrs = new HashSet<Address>();
      secAdrs.add(secAdr1);
      secAdrs.add(secAdr2);

      HashSet<String> nickNames = new HashSet<String>();
      nickNames.add("nick1");
      nickNames.add("nick2");

      Person person = new Person();
      person.setId(key);
      person.setName("test person");

      person.setNickNames(nickNames);
      person.setAddress(adr);
      person.setSecondaryAdresses(secAdrs);

      return new TestObject(person.getId(), person);
   }
}
