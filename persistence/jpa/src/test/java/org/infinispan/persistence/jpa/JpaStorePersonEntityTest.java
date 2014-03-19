package org.infinispan.persistence.jpa;

import org.infinispan.persistence.jpa.entity.Address;
import org.infinispan.persistence.jpa.entity.Person;
import org.mockito.internal.util.collections.Sets;
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

      Person person = new Person();
      person.setId(key);
      person.setName("test person");
      person.setNickNames(Sets.newSet("nick1", "nick2"));
      person.setAddress(adr);
      person.setSecondaryAdresses(Sets.newSet(secAdr1, secAdr2));

      return new TestObject(person.getId(), person);
   }
}
