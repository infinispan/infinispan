package org.infinispan.all.embeddedquery.testdomain;

import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface User {

   enum Gender {
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

   String getSalutation();

   void setSalutation(String salutation);

   Integer getAge();

   void setAge(Integer age);

   Gender getGender();

   void setGender(Gender gender);

   List<Address> getAddresses();

   void setAddresses(List<Address> addresses);

   String getNotes();

   void setNotes(String notes);

   Instant getCreationDate();

   void setCreationDate(Instant creationDate);

   Instant getPasswordExpirationDate();

   void setPasswordExpirationDate(Instant passwordExpirationDate);
}
