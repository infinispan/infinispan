package org.infinispan.client.hotrod.configuration;

/**
 * SaslStrength. Possible values for the SASL strength property.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public enum SaslStrength {
   LOW("low"), MEDIUM("medium"), HIGH("high");

   private String v;

   SaslStrength(String v) {
      this.v = v;
   }

   @Override
   public String toString() {
      return v;
   }
}
