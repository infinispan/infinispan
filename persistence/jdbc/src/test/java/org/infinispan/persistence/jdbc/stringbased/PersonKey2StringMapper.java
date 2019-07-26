package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.persistence.keymappers.Key2StringMapper;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;

/**
 * Used for testing jdbc cache stores.
 *
 * @author Mircea.Markus@jboss.com
 */
public class PersonKey2StringMapper implements Key2StringMapper {
   public boolean isSupportedType(Class keyType) {
      return keyType == Person.class;
   }

   public String getStringMapping(Object key) {
      Person person = (Person) key;
      Address addr = person.getAddress();
      if (addr == null)
         return person.getName();

      return person.getName() + "_" + addr.getStreet() + "_" + addr.getCity() + "_" + addr.getZip();
   }
}
