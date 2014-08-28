package org.infinispan.query.dsl.embedded.testdomain;

import java.util.List;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface User {

   public enum Gender {
      MALE, FEMALE
   }

   int getId();

   void setId(int id);

   Set<Integer> getAccountIds();

   void setAccountIds(Set<Integer> accountIds);

   String getName();

   void setName(String name);

   String getSurname();

   void setSurname(String surname);

   Integer getAge();

   void setAge(Integer age);

   Gender getGender();

   void setGender(Gender gender);

   List<Address> getAddresses();

   void setAddresses(List<Address> addresses);

   String getNotes();

   void setNotes(String notes);
}
