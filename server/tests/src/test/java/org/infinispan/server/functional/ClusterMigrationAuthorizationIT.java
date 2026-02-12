package org.infinispan.server.functional;

import org.infinispan.server.test.api.TestUser;
import org.infinispan.util.KeyValuePair;

/**
 * @since 15.0
 */
public class ClusterMigrationAuthorizationIT extends ClusterMigrationDynamicStoreIT {

   @Override
   protected String configFile() {
      return "configuration/AuthorizationImplicitTest.xml";
   }

   @Override
   protected KeyValuePair<String, String> getCredentials() {
      return new KeyValuePair<>(TestUser.ADMIN.getUser(), TestUser.ADMIN.getPassword());
   }
}
