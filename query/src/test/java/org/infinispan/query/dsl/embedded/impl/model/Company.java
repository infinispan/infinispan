package org.infinispan.query.dsl.embedded.impl.model;

import java.util.Set;

import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.api.annotations.indexing.option.Structure;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class Company {

   private Long id;

   private String name;

   private Set<Employee> employees;

   private Address address;

   @Keyword(projectable = true)
   public String getName() {
      return name;
   }

   public Set<Employee> getEmployees() {
      return employees;
   }

   @Embedded(structure = Structure.FLATTENED)
   public Address getAddress() {
      return address;
   }

   public class Address {

      public Long id;

      public String street;

      public String city;

      public Set<Company> companies;

      @Text(projectable = true)
      public String getStreet() {
         return street;
      }

      @Text
      public String getCity() {
         return city;
      }

      public Set<Company> getCompanies() {
         return companies;
      }
   }
}
