package org.infinispan.commons.configuration;

/**
 * A simple wrapper for a configuration represented as a String.
 *
 * @author Tristan Tarrant
 * @since 14.0
 */
public class StringConfiguration implements BasicConfiguration {
   private final String string;

   public StringConfiguration(String string) {
      this.string = string;
   }

   @Override
   public String toStringConfiguration(String name) {
      return string;
   }
}
