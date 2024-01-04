package org.infinispan.objectfilter.test.model;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class PhoneNumber {

   private String number;

   // this is protected in order to demonstrate that direct field access will be used instead of method access
   @ProtoField(1)
   protected String getNumber() {
      return number;
   }

   public void setNumber(String number) {
      this.number = number;
   }

   @Override
   public String toString() {
      return "PhoneNumber{" +
            "number='" + number + '\'' +
            '}';
   }
}
