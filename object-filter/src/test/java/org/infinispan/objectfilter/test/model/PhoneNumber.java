package org.infinispan.objectfilter.test.model;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class PhoneNumber {

   private String number;

   public String getNumber() {
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
