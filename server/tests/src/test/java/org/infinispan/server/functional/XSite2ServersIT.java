package org.infinispan.server.functional;

import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;

import org.infinispan.server.functional.rest.XSiteRestMetricsOperations2;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.infinispan.server.test.junit5.InfinispanXSiteServerExtension;
import org.infinispan.server.test.junit5.InfinispanXSiteServerExtensionBuilder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Cross-Site suite with 2 servers per site and a single site master per site.
 */
@Suite(failIfNoTests = false)
@SelectClasses({
      XSiteRestMetricsOperations2.class
})
public class XSite2ServersIT extends InfinispanSuite {

   public static final int NUM_SERVERS = 2;

   public static final String LON_CACHE_CONFIG =
         "<replicated-cache name=\"%s\">" +
               "     <backups>" +
               "        <backup site=\"NYC\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "</replicated-cache>";

   public static final String NYC_CACHE_CONFIG =
         "<replicated-cache name=\"%s\">" +
               "     <backups>" +
               "        <backup site=\"LON\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "</replicated-cache>";

   static InfinispanServerExtensionBuilder lonServerRule = InfinispanServerExtensionBuilder
         .config("configuration/XSiteServerTestSingleSiteMaster.xml")
         .runMode(ServerRunMode.CONTAINER)
         .parallelStartup(false)
         .numServers(NUM_SERVERS);

   static InfinispanServerExtensionBuilder nycServerRule = InfinispanServerExtensionBuilder
         .config("configuration/XSiteServerTestSingleSiteMaster.xml")
         .runMode(ServerRunMode.CONTAINER)
         .parallelStartup(false)
         .numServers(NUM_SERVERS);

   @RegisterExtension
   public static final InfinispanXSiteServerExtension SERVERS = new InfinispanXSiteServerExtensionBuilder()
         .addSite(LON, lonServerRule)
         .addSite(NYC, nycServerRule)
         .build();
}
