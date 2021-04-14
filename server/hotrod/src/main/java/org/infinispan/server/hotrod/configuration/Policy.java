package org.infinispan.server.hotrod.configuration;

import static java.util.Arrays.stream;

import org.infinispan.commons.configuration.io.NamingStrategy;

/**
 * @since 13.0
 */
public enum Policy {
   FORWARD_SECRECY,
   NO_ACTIVE,
   NO_ANONYMOUS,
   NO_DICTIONARY,
   NO_PLAIN_TEXT,
   PASS_CREDENTIALS;

   private final String name;

   Policy() {
      this.name = NamingStrategy.KEBAB_CASE.convert(name().toLowerCase());
   }

   @Override
   public String toString() {
      return name;
   }

   public static Policy fromString(String s) {
      return stream(Policy.values())
            .filter(p -> p.name.equalsIgnoreCase(s))
            .findFirst().orElse(null);
   }

}
