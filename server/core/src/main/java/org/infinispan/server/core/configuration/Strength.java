package org.infinispan.server.core.configuration;

import static java.util.Arrays.stream;

/**
 * @since 10.0
 */
public enum Strength {
   LOW("low"), MEDIUM("medium"), HIGH("high");

   private final String str;

   Strength(String str) {
      this.str = str;
   }

   @Override
   public String toString() {
      return str;
   }

   public static Strength fromString(String s) {
      return stream(Strength.values())
            .filter(q -> q.str.equalsIgnoreCase(s))
            .findFirst().orElse(null);
   }

}
