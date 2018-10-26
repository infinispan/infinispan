package org.infinispan.all.embeddedquery.testdomain.hsearch;

import java.io.Serializable;
import java.util.Objects;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Store;
import org.infinispan.all.embeddedquery.testdomain.Address;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class AddressHS implements Address, Serializable {

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String street;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String postCode;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private int number;

   private boolean isCommercial;

   @Override
   public String getStreet() {
      return street;
   }

   @Override
   public void setStreet(String street) {
      this.street = street;
   }

   @Override
   public String getPostCode() {
      return postCode;
   }

   @Override
   public void setPostCode(String postCode) {
      this.postCode = postCode;
   }

   @Override
   public int getNumber() {
      return number;
   }

   @Override
   public void setNumber(int number) {
      this.number = number;
   }

   @Override
   public boolean isCommercial() {
      return isCommercial;
   }

   @Override
   public void setCommercial(boolean isCommercial) {
      this.isCommercial = isCommercial;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddressHS address = (AddressHS) o;
      return number == address.number &&
            isCommercial == address.isCommercial &&
            Objects.equals(street, address.street) &&
            Objects.equals(postCode, address.postCode);
   }

   @Override
   public int hashCode() {
      return Objects.hash(street, postCode, number, isCommercial);
   }

   @Override
   public String toString() {
      return "AddressHS{" +
            "street='" + street + '\'' +
            ", postCode='" + postCode + '\'' +
            ", number=" + number +
            ", isCommercial=" + isCommercial +
            '}';
   }
}
