package org.infinispan.hotrod.configuration;

/**
 * SaslStrength. Possible values for the SASL strength property.
 *
 * @since 14.0
 */
public enum SaslStrength {
   LOW("low"), MEDIUM("medium"), HIGH("high");

   private final String v;

   SaslStrength(String v) {
      this.v = v;
   }

   @Override
   public String toString() {
      return v;
   }
}
