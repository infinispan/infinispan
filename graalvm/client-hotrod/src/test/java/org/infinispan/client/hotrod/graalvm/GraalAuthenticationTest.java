package org.infinispan.client.hotrod.graalvm;

import org.infinispan.server.security.authentication.HotRodAuthentication;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.jupiter.InfinispanServerExtension;
import org.infinispan.server.test.jupiter.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.jupiter.InfinispanSuite;
import org.infinispan.testing.jupiter.tags.Security;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 16.3
 **/
@Suite(failIfNoTests = false)
@SelectClasses({HotRodAuthentication.class})
@Security
public class GraalAuthenticationTest extends InfinispanSuite {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationServerTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .numServers(2)
               .build();
}
