package org.infinispan.persistence.jdbc.stringbased;

import java.util.StringTokenizer;

import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TwoWayPersonKey2StringMapper extends PersonKey2StringMapper implements TwoWayKey2StringMapper {

   @Override
   public Object getKeyMapping(String key) {
      StringTokenizer tkz = new StringTokenizer(key, "_");
      String name = tkz.nextToken();
      if (!tkz.hasMoreTokens())
         return new Person(name);

      String street = tkz.nextToken();
      String city = tkz.nextToken();
      String zip = tkz.nextToken();
      return new Person(name, new Address(street, city, Integer.parseInt(zip)));
   }
}
