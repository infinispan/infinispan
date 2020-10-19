package org.infinispan.rest.search.entity;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * @since 9.2
 */
public class PhoneNumber {

   private String number;

   @ProtoField(1)
   public String getNumber() {
      return number;
   }

   public void setNumber(String number) {
      this.number = number;
   }
}
