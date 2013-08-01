package org.infinispan.query.dsl.sample_domain_model;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
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

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String surname;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private int age;  // yes, not the birth date :)

   @Field(store = Store.YES, analyze = Analyze.NO)
   private Gender gender;

   @IndexedEmbedded
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
