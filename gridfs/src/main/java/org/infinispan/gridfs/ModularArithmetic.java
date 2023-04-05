package org.infinispan.gridfs;

/**
 * For compatibility
 *
 * @author Manik Surtani
 */
public class ModularArithmetic {
   public static final boolean CANNOT_ASSUME_DENOM_IS_POWER_OF_TWO = Boolean.getBoolean("infinispan.compat");

   public static int mod(int numerator, int denominator) {
      if (CANNOT_ASSUME_DENOM_IS_POWER_OF_TWO)
         return numerator % denominator;
      else
         return numerator & (denominator - 1);
   }

   public static long mod(long numerator, int denominator) {
      if (CANNOT_ASSUME_DENOM_IS_POWER_OF_TWO)
         return numerator % denominator;
      else
         return numerator & (denominator - 1);
   }

}
