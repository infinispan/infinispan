package org.infinispan.server.security.authentication;

import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.category.Security;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({HotRodAuthentication.class, RestAuthentication.class})
@Category(Security.class)
public class AuthenticationIT {

   @ClassRule
   public static final InfinispanServerRule SERVERS = new InfinispanServerRule(
         new InfinispanServerRuleConfigurationBuilder("configuration/AuthenticationServerTest.xml")
               .numServers(2)
   );
}
