package org.infinispan.server.functional;

import org.infinispan.util.KeyValuePair;

/**
 * @since 12.1
 */
public class RollingUpgradeSecureIT extends RollingUpgradeIT {

   static final String USER = "all_user";
   static final String PASS = "all";

   public RollingUpgradeSecureIT() {
      super("configuration/AuthenticationServerTest.xml");
   }

   @Override
   protected KeyValuePair<String, String> getCredentials() {
      return new KeyValuePair<>(USER, PASS);
   }

}
