package org.infinispan.server.backwardcompatibility;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 11.0
 **/

import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
      HotrodClientBackwardCompatibility.class
})
public class BackwardCompatibilityIT {

   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .runMode(ServerRunMode.FORKED)
               .numServers(1)
               .build();
}