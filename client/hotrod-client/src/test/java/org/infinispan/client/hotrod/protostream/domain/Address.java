package org.infinispan.client.hotrod.protostream.domain;

import org.infinispan.protostream.BaseMessage;

/**
 * @author anistor@redhat.com
 */
public class Address extends BaseMessage {

   private String street;
   private String postCode;

   public Address() {
   }

   public Address(String street, String postCode) {
      this.street = street;
      this.postCode = postCode;
   }

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
            ", unknownFieldSet='" + unknownFieldSet + '\'' +
            '}';
   }
}
