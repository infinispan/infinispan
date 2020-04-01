package org.infinispan.server.security.authorization;

import org.infinispan.server.test.core.LdapServerRule;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@Category(Security.class)
public class AuthorizationLDAPIT extends AbstractAuthorization {
   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthorizationLDAPTest.xml")
               .build();

   @ClassRule
   public static LdapServerRule LDAP = new LdapServerRule(SERVERS);

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);


   @Override
   protected InfinispanServerTestMethodRule getServerTest() {
      return SERVER_TEST;
   }
}
