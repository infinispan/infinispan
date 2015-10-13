package org.infinispan.util;

import java.io.Serializable;

/**
 * {@link Serializable} {@link Integer} wrapper.
 */
public final class Int implements Serializable {
   private final int aInt;

   public Int(int aInt) {
      this.aInt = aInt;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Int intKey = (Int) o;

      return aInt == intKey.aInt;

   }

   @Override
   public int hashCode() {
      return aInt;
   }

   @Override
   public String toString() {
      return "Int=" + aInt;
   }
}