package org.infinispan.query.dsl.sample_domain_model;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.annotations.NumericField;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.impl.BuiltinIterableBridge;

import java.util.List;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Indexed
public class User {

   public enum Gender {
      MALE, FEMALE
   }

   @Field(store = Store.YES, analyze = Analyze.NO)
   private int id;

   @Field(store = Store.YES, analyze = Analyze.NO)
   @FieldBridge(impl = BuiltinIterableBridge.class)
   private Set<Integer> accountIds;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String name;

   @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
   private String surname;

   @Field(store = Store.YES, analyze = Analyze.NO)
   @NumericField
   private int age;  // yes, not the birth date :)

   @Field(store = Store.YES, analyze = Analyze.NO)
   private Gender gender;

   @IndexedEmbedded(indexNullAs = Field.DEFAULT_NULL_TOKEN)
   private List<Address> addresses;

   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   public Set<Integer> getAccountIds() {
      return accountIds;
   }

   public void setAccountIds(Set<Integer> accountIds) {
      this.accountIds = accountIds;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getSurname() {
      return surname;
   }

   public void setSurname(String surname) {
      this.surname = surname;
   }

   public int getAge() {
      return age;
   }

   public void setAge(int age) {
      this.age = age;
   }

   public Gender getGender() {
      return gender;
   }

   public void setGender(Gender gender) {
      this.gender = gender;
   }

   public List<Address> getAddresses() {
      return addresses;
   }

   public void setAddresses(List<Address> addresses) {
      this.addresses = addresses;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      User user = (User) o;

      if (age != user.age) return false;
      if (id != user.id) return false;
      if (accountIds != null ? !accountIds.equals(user.accountIds) : user.accountIds != null) return false;
      if (addresses != null ? !addresses.equals(user.addresses) : user.addresses != null) return false;
      if (gender != user.gender) return false;
      if (name != null ? !name.equals(user.name) : user.name != null) return false;
      if (surname != null ? !surname.equals(user.surname) : user.surname != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = id;
      result = 31 * result + (accountIds != null ? accountIds.hashCode() : 0);
      result = 31 * result + (name != null ? name.hashCode() : 0);
      result = 31 * result + (surname != null ? surname.hashCode() : 0);
      result = 31 * result + age;
      result = 31 * result + (gender != null ? gender.hashCode() : 0);
      result = 31 * result + (addresses != null ? addresses.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "User{" +
            "id=" + id +
            ", name='" + name + '\'' +
            ", surname='" + surname + '\'' +
            ", accountIds=" + accountIds +
            ", addresses=" + addresses +
            ", age=" + age +
            ", gender=" + gender +
            '}';
   }
}
