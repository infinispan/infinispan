package org.infinispan.rest.search.entity;

import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * @since 9.2
 */
@SuppressWarnings("unused")
public class PhoneNumber implements Serializable {

   private String number;

   @ProtoField(number = 1)
   public String getNumber() {
      return number;
   }

   public void setNumber(String number) {
      this.number = number;
   }
}
