package org.infinispan.query.test;

import java.io.Serializable;

import org.infinispan.marshall.core.ExternalPojo;

public class CustomKey3 implements Serializable, ExternalPojo {

   private static final long serialVersionUID = -1;

   private String str;

   public CustomKey3() {
   }

   public CustomKey3(String str) {
      this.str = str;
   }

   public String getStr() {
      return str;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CustomKey3 that = (CustomKey3) o;
      return str != null ? str.equals(that.str) : that.str == null;
   }

   @Override
   public int hashCode() {
      return str != null ? str.hashCode() : 0;
   }
}
