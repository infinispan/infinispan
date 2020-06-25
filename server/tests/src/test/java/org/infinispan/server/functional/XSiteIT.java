package org.infinispan.server.functional;

import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Cross-Site suite
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
      XSiteHotRodCacheOperations.class,
      XSiteRestCacheOperations.class
})
public class XSiteIT {
   @ClassRule
   public static final InfinispanServerRule LON_SERVERS =
         InfinispanServerRuleBuilder.config("configuration/XSiteServerTest.xml")
               .numServers(3)
               .site(InfinispanServerTestConfiguration.LON)
               .build();
   @ClassRule
   public static final InfinispanServerRule NYC_SERVERS =
         InfinispanServerRuleBuilder.config("configuration/XSiteServerTest.xml")
               .numServers(3)
               .site(InfinispanServerTestConfiguration.NYC)
               .build();
}
