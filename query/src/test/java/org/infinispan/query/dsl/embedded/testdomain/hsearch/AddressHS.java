package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Store;
import org.infinispan.query.dsl.embedded.testdomain.Address;

import java.io.Serializable;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class AddressHS implements Address, Serializable {

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

      AddressHS other = (AddressHS) o;

      if (postCode != null ? !postCode.equals(other.postCode) : other.postCode != null) return false;
      if (street != null ? !street.equals(other.street) : other.street != null) return false;

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
      return "AddressHS{" +
            "street='" + street + '\'' +
            ", postCode='" + postCode + '\'' +
            '}';
   }
}
