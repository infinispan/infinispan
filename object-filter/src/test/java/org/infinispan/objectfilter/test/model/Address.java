package org.infinispan.objectfilter.test.model;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class Address {

   private String street;

   private String postCode;

   @ProtoField(1)
   public String getStreet() {
      return street;
   }

   public void setStreet(String street) {
      this.street = street;
   }

   @ProtoField(2)
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
