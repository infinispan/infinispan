package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.annotations.NumericField;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.impl.BuiltinIterableBridge;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Indexed
public class UserHS implements User, Serializable {

   @Field(store = Store.YES, analyze = Analyze.NO)
   private int id;

   @Field(store = Store.YES, analyze = Analyze.NO)
   @FieldBridge(impl = BuiltinIterableBridge.class)
   private Set<Integer> accountIds;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String name;

   @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
   private String surname;

   @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
   @NumericField
   private Integer age;  // yes, not the birth date :)

   @Field(store = Store.YES, analyze = Analyze.NO)
   private Gender gender;

   @IndexedEmbedded(targetElement = AddressHS.class, indexNullAs = Field.DEFAULT_NULL_TOKEN)
   private List<Address> addresses;

   /**
    * This is not indexed!
    */
   private String notes;

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

   public Integer getAge() {
      return age;
   }

   public void setAge(Integer age) {
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
   public String getNotes() {
      return notes;
   }

   @Override
   public void setNotes(String notes) {
      this.notes = notes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      UserHS other = (UserHS) o;

      if (age != null ? !age.equals(other.age) : other.age != null) return false;
      if (id != other.id) return false;
      if (accountIds != null ? !accountIds.equals(other.accountIds) : other.accountIds != null) return false;
      if (addresses != null ? !addresses.equals(other.addresses) : other.addresses != null) return false;
      if (gender != other.gender) return false;
      if (name != null ? !name.equals(other.name) : other.name != null) return false;
      if (surname != null ? !surname.equals(other.surname) : other.surname != null) return false;
      if (notes != null ? !notes.equals(other.notes) : other.notes != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = id;
      result = 31 * result + (accountIds != null ? accountIds.hashCode() : 0);
      result = 31 * result + (name != null ? name.hashCode() : 0);
      result = 31 * result + (surname != null ? surname.hashCode() : 0);
      result = 31 * result + (age != null ? age : 0);
      result = 31 * result + (gender != null ? gender.hashCode() : 0);
      result = 31 * result + (addresses != null ? addresses.hashCode() : 0);
      result = 31 * result + (notes != null ? notes.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "UserHS{" +
            "id=" + id +
            ", name='" + name + '\'' +
            ", surname='" + surname + '\'' +
            ", accountIds=" + accountIds +
            ", addresses=" + addresses +
            ", age=" + age +
            ", gender=" + gender +
            ", notes=" + notes +
            '}';
   }

}
