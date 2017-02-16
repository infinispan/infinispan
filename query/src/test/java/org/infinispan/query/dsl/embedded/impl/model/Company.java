package org.infinispan.query.dsl.embedded.impl.model;

import java.util.Set;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.ContainedIn;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.annotations.Store;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class Company {

   private Long id;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String name;

   @ContainedIn
   private Set<Employee> employees;

   @IndexedEmbedded
   private Address address;

   public class Address {

      public Long id;

      @Field(store = Store.YES)
      public String street;

      @Field
      public String city;

      @ContainedIn
      public Set<Company> companies;
   }
}
