package org.infinispan.server.security.authorization;

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
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);


   @Override
   protected InfinispanServerTestMethodRule getServerTest() {
      return SERVER_TEST;
   }
}
