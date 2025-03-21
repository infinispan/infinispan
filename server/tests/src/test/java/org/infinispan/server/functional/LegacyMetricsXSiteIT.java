package org.infinispan.server.functional;

import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;

import org.infinispan.server.functional.rest.LegacyXSiteRestMetricsOperations;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.infinispan.server.test.junit5.InfinispanXSiteServerExtension;
import org.infinispan.server.test.junit5.InfinispanXSiteServerExtensionBuilder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Cross-Site suite
 *
 * @author Pedro Ruivo
 * @author Gustavo Lira
 * @since 11.0
 */
@Deprecated(forRemoval = true, since = "15.2")
@Suite(failIfNoTests = false)
@SelectClasses({
      LegacyXSiteRestMetricsOperations.class
})
public class LegacyMetricsXSiteIT extends InfinispanSuite {

   public static final int NUM_SERVERS = 1;

   static InfinispanServerExtensionBuilder lonServerRule = InfinispanServerExtensionBuilder
         .config("configuration/LegacyMetricsXSiteServerTest.xml")
         .runMode(ServerRunMode.CONTAINER)
         .numServers(NUM_SERVERS);

   static InfinispanServerExtensionBuilder nycServerRule = InfinispanServerExtensionBuilder
         .config("configuration/LegacyMetricsXSiteServerTest.xml")
         .runMode(ServerRunMode.CONTAINER)
         .numServers(NUM_SERVERS);

   @RegisterExtension
   public static final InfinispanXSiteServerExtension SERVERS = new InfinispanXSiteServerExtensionBuilder()
            .addSite(LON, lonServerRule)
            .addSite(NYC, nycServerRule)
            .build();
}
