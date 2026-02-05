package org.infinispan.commons;

import java.security.KeyPairGenerator;

import org.infinispan.commons.logging.Log;

public final class FIPS {
   private static final boolean ENABLED;

   static {
      boolean enabled = false;
      try {
         KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
         kpg.initialize(512); // FIPS requires >= 2048 bits
         kpg.generateKeyPair();
      } catch (Throwable t) {
         enabled = true;
      }
      ENABLED = enabled;
      Log.SECURITY.infof("FIPS=%s", ENABLED ? "enabled" : "disabled");
   }

   public static boolean isFipsEnabled() {
      return ENABLED;
   }
}
