package org.infinispan.commons.equivalence;

import java.util.Arrays;

/**
 * A compare function for unsigned byte arrays.
 *
 * @author Galder Zamarreño
 * @since 5.3
 * @deprecated
 */
public class ByteArrayEquivalence implements Equivalence<byte[]> {

   public static final Equivalence<byte[]> INSTANCE = new ByteArrayEquivalence();

   @Override
   public int hashCode(Object obj) {
      return 41 + Arrays.hashCode((byte[]) obj);
   }

   @Override
   public boolean equals(byte[] obj, Object otherObj) {
      if (obj == otherObj) return true;
      if (obj == null) return false;
      if (otherObj == null || byte[].class != otherObj.getClass()) return false;
      byte[] otherByteArray = (byte[]) otherObj;
      return Arrays.equals(obj, otherByteArray);
   }

   @Override
   public String toString(Object obj) {
      return Arrays.toString((byte[]) obj);
   }

   @Override
   public boolean isComparable(Object obj) {
      return true;
   }

   @Override
   public int compare(byte[] obj, byte[] otherObj) {
      // Assumes unsigned byte arrays
      int minLength = Math.min(obj.length, otherObj.length);
      for (int i = 0; i < minLength; i++) {
         int compareResult = obj[i] - otherObj[i];
         if (compareResult != 0)
            return compareResult;
      }
      return obj.length - otherObj.length;
   }

}
