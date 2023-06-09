package org.infinispan.server.security.authorization;

import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/

@Category(Security.class)
public class AuthorizationPropertiesIT extends AbstractAuthorization {
   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationPropertiesTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(ClusteredIT.mavenArtifacts())
               .artifacts(ClusteredIT.artifacts())
               .build();

   @Override
   protected InfinispanServerExtension getServers() {
      return SERVERS;
   }
}
