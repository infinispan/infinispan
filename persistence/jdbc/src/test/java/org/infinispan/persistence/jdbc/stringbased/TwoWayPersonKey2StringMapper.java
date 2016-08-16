package org.infinispan.persistence.jdbc.stringbased;

import java.util.StringTokenizer;

import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TwoWayPersonKey2StringMapper extends PersonKey2StringMapper implements TwoWayKey2StringMapper {

   @Override
   public Object getKeyMapping(String key) {
      //person.getName() + "_" + person.getSurname() + "_" + person.getAge();
      StringTokenizer tkz = new StringTokenizer(key, "_");
      String name = tkz.nextToken();
      String surname = tkz.nextToken();
      String age = tkz.nextToken();
      return new Person(name, surname, Integer.parseInt(age));
   }
}
