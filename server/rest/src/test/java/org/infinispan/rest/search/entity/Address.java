package org.infinispan.rest.search.entity;

import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * @since 9.2
 */
@SuppressWarnings("unused")
public class Address implements Serializable {

   private String street;
   private String postCode;

   @ProtoField(number = 1)
   public String getStreet() {
      return street;
   }

   public void setStreet(String street) {
      this.street = street;
   }

   @ProtoField(number = 2)
   public String getPostCode() {
      return postCode;
   }

   public void setPostCode(String postCode) {
      this.postCode = postCode;
   }
}
