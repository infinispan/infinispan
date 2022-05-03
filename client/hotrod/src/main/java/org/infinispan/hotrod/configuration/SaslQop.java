package org.infinispan.hotrod.configuration;

/**
 * SaslQop. Possible values for the SASL QOP property
 *
 * @since 14.0
 */
public enum SaslQop {
   AUTH("auth"), AUTH_INT("auth-int"), AUTH_CONF("auth-conf");

   private String v;

   SaslQop(String v) {
      this.v = v;
   }

   @Override
   public String toString() {
      return v;
   }

}
