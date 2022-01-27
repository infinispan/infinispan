package org.infinispan.server.security.authentication;

import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({HotRodAuthentication.class, RestAuthentication.class, RespAuthentication.class})
@Category(Security.class)
public class AuthenticationIT {

   @ClassRule
   public static final InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthenticationServerTest.xml")
                                    .numServers(2)
                                    .build();
}
