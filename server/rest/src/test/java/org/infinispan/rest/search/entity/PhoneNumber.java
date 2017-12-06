package org.infinispan.rest.search.entity;

import java.io.Serializable;

import org.infinispan.marshall.core.ExternalPojo;

/**
 * @since 9.2
 */
@SuppressWarnings("unused")
public class PhoneNumber implements Serializable, ExternalPojo {

   private String number;

   public String getNumber() {
      return number;
   }

   public void setNumber(String number) {
      this.number = number;
   }
}
