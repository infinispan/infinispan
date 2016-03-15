package org.infinispan.query.test;

import java.io.Serializable;

public class CustomKey3 implements Serializable {
   private static final long serialVersionUID = -8825579871900146417L;

   String str;

   public CustomKey3() {
   }

   public CustomKey3(String str) {
      this.str = str;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomKey3 that = (CustomKey3) o;

      if (str != null ? !str.equals(that.str) : that.str != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return str != null ? str.hashCode() : 0;
   }
}
