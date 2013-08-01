package org.infinispan.query.dsl.sample_domain_model;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Indexed
public class Address {

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String street;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String postCode;

   public String getStreet() {
      return street;
   }

   public void setStreet(String street) {
      this.street = street;
   }

   public String getPostCode() {
      return postCode;
   }

   public void setPostCode(String postCode) {
      this.postCode = postCode;
   }

   @Override
   public String toString() {
      return "Address{" +
            "street='" + street + '\'' +
            ", postCode='" + postCode + '\'' +
            '}';
   }
}
