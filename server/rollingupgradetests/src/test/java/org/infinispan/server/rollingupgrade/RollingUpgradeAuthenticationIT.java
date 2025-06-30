package org.infinispan.server.rollingupgrade;

import org.infinispan.server.security.authentication.HotRodAuthentication;
import org.infinispan.server.security.authentication.MemcachedAuthentication;
import org.infinispan.server.security.authentication.RespAuthentication;
import org.infinispan.server.security.authentication.RestAuthentication;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.infinispan.server.test.junit5.RollingUpgradeHandlerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * @author William Burns &lt;will@infinispan.org&gt;
 * @since 16.0
 **/
@Suite(failIfNoTests = false)
@SelectClasses({HotRodAuthentication.class, RestAuthentication.class, MemcachedAuthentication.class, RespAuthentication.class})
@Security
public class RollingUpgradeAuthenticationIT extends InfinispanSuite {

   static {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(RollingUpgradeAuthenticationIT.class.getName(), "15.2.0.Final", "15.2.1.Final")
            .nodeCount(3)
            .configurationUpdater(cb -> {
               cb.security().authentication()
                     .username(TestUser.ADMIN.getUser())
                     .password(TestUser.ADMIN.getPassword());
               return cb;
            })
            .useCustomServerConfiguration("configuration/AuthenticationServerTest.xml");
      SERVERS = new RollingUpgradeHandlerExtension(builder);
   }

   @RegisterExtension
   public static final RollingUpgradeHandlerExtension SERVERS;
}
