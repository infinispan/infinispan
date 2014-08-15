package org.infinispan.persistence.jpa.entity;

import java.io.Serializable;
import java.util.Set;

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;

@Entity
public class Person implements Serializable {

   private static final long serialVersionUID = 4748311041613897465L;

   @Id
   private String id;

   private String name;

   @ElementCollection(fetch = FetchType.EAGER)
   private Set<String> nickNames;

   @Embedded
   @AttributeOverrides({ @AttributeOverride(name = "zipCode", column = @Column(name = "zip")) })
   private Address address;

   @ElementCollection(fetch = FetchType.EAGER)
   private Set<Address> secondaryAdresses;

   public String getId() {
      return id;
   }

   public void setId(String id) {
      this.id = id;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public Set<String> getNickNames() {
      return nickNames;
   }

   public void setNickNames(Set<String> nickNames) {
      this.nickNames = nickNames;
   }

   public Address getAddress() {
      return address;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   public Set<Address> getSecondaryAdresses() {
      return secondaryAdresses;
   }

   public void setSecondaryAdresses(Set<Address> secondaryAdresses) {
      this.secondaryAdresses = secondaryAdresses;
   }

   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      final Person person = (Person) o;

      if (id != null ? !id.equals(person.getId()) : person.getId() != null)
         return false;
      if (name != null ? !name.equals(person.getName()) : person.getName() != null)
         return false;
      if ( (nickNames != null && !nickNames.isEmpty()) ? !nickNames.equals(person.getNickNames()) : 
         (person.getNickNames() != null && !person.getNickNames().isEmpty()))
         return false;
      if (address != null ? !address.equals(person.getAddress()) : person.getAddress() != null)
         return false;
      if ( (secondaryAdresses != null  && !secondaryAdresses.isEmpty() ) ? !secondaryAdresses.equals(person.getSecondaryAdresses()) : 
         (person.getSecondaryAdresses() != null && !person.getSecondaryAdresses().isEmpty()))
         return false;

      return true;
   }

   public int hashCode() {
      final int prime = 31;
      int result;
      result = (id != null ? id.hashCode() : 0);
      result = prime * result + (name != null ? name.hashCode() : 0);
      result = prime * result + (nickNames != null ? nickNames.hashCode() : 0);
      result = prime * result + (address != null ? address.hashCode() : 0);
      result = prime * result + (secondaryAdresses != null ? secondaryAdresses.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "Person{" +
            "id='" + id + '\'' +
            ", name='" + name + '\'' +
            ", nickNames=" + nickNames +
            ", address=" + address +
            ", secondaryAdresses=" + secondaryAdresses +
            '}';
   }
}
