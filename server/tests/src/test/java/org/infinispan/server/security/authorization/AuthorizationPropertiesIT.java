package org.infinispan.server.security.authorization;

import org.infinispan.server.extensions.ExtensionsIT;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/

@Category(Security.class)
public class AuthorizationPropertiesIT extends AbstractAuthorization {
   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthorizationPropertiesTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .artifacts(ExtensionsIT.artifacts())
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Override
   protected InfinispanServerRule getServers() {
      return SERVERS;
   }

   @Override
   protected InfinispanServerTestMethodRule getServerTest() {
      return SERVER_TEST;
   }
}
