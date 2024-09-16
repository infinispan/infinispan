package org.infinispan.server.security.authentication;

import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Suite(failIfNoTests = false)
@SelectClasses({HotRodAuthentication.class, RestAuthentication.class, MemcachedAuthentication.class, RespAuthentication.class})
@Security
public class AuthenticationIT extends InfinispanSuite {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationServerTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .numServers(2)
               .build();
}
