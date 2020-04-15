package org.infinispan.query.dsl.embedded.impl.model;

import java.util.Set;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.annotations.IndexedEmbedded;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class Company {

   private Long id;

   private String name;

   private Set<Employee> employees;

   private Address address;

   @Field(store = Store.YES, analyze = Analyze.NO)
   public String getName() {
      return name;
   }

   public Set<Employee> getEmployees() {
      return employees;
   }

   @IndexedEmbedded
   public Address getAddress() {
      return address;
   }

   public class Address {

      public Long id;

      public String street;

      public String city;

      public Set<Company> companies;

      @Field(store = Store.YES)
      public String getStreet() {
         return street;
      }

      @Field
      public String getCity() {
         return city;
      }

      public Set<Company> getCompanies() {
         return companies;
      }
   }
}
