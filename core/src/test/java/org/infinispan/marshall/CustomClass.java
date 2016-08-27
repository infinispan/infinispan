package org.infinispan.marshall;

import java.io.Serializable;

public class CustomClass implements Serializable {
   final String val;

   public CustomClass(String val) {
      this.val = val;
   }

   public String getVal() {
      return val;
   }

   @Override
   public String toString() {
      return "CustomClass{" +
            "val='" + val + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomClass that = (CustomClass) o;

      if (val != null ? !val.equals(that.val) : that.val != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return val != null ? val.hashCode() : 0;
   }
}
