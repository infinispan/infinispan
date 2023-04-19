package org.infinispan.graalvm.substitutions.graal;

public class Util {
   public static UnsupportedOperationException unsupportedOperationException(String feature) {
      return unsupportedOperationException(feature, "");
   }

   public static UnsupportedOperationException unsupportedOperationException(String feature, String explanation) {
      return new UnsupportedOperationException(String.format("'%s'is not supported in native runtime!%s", feature, explanation));
   }
}
