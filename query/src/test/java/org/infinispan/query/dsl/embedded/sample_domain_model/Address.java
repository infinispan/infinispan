package org.infinispan.query.dsl.embedded.sample_domain_model;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

import java.io.Serializable;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Indexed
public class Address implements Serializable {

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
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Address address = (Address) o;

      if (postCode != null ? !postCode.equals(address.postCode) : address.postCode != null) return false;
      if (street != null ? !street.equals(address.street) : address.street != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = street != null ? street.hashCode() : 0;
      result = 31 * result + (postCode != null ? postCode.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "Address{" +
            "street='" + street + '\'' +
            ", postCode='" + postCode + '\'' +
            '}';
   }
}
