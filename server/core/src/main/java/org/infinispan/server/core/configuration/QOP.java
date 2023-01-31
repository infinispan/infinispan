package org.infinispan.server.core.configuration;

import static java.util.Arrays.stream;

/**
 * @since 10.0
 */
public enum QOP {
   AUTH("auth"),
   AUTH_INT("auth-int"),
   AUTH_CONF("auth-conf");

   private String v;

   QOP(String v) {
      this.v = v;
   }

   @Override
   public String toString() {
      return v;
   }

   public static QOP fromString(String s) {
      return stream(QOP.values())
            .filter(q -> q.v.equalsIgnoreCase(s))
            .findFirst().orElse(null);
   }

}
