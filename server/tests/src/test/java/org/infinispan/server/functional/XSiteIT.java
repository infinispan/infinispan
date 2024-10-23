package org.infinispan.server.functional;

import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;

import org.infinispan.server.cli.XSiteCliOperations;
import org.infinispan.server.functional.hotrod.XSiteHotRodCacheOperations;
import org.infinispan.server.functional.rest.XSiteRestCacheOperations;
import org.infinispan.server.functional.rest.XSiteRestMetricsOperations;
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
@Suite(failIfNoTests = false)
@SelectClasses({
      XSiteRestMetricsOperations.class,
      XSiteHotRodCacheOperations.class,
      XSiteRestCacheOperations.class,
      XSiteCliOperations.class
})
public class XSiteIT extends InfinispanSuite {

   public static final int NUM_SERVERS = 1;

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

   public static final String LON_CACHE_CUSTOM_NAME_CONFIG =
         "<replicated-cache name=\"lon-cache-%s\">" +
               "     <backups>" +
               "        <backup site=\"NYC\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "     <backup-for remote-cache=\"nyc-cache-%s\" remote-site=\"NYC\" />" +
               "</replicated-cache>";

   public static final String NYC_CACHE_CUSTOM_NAME_CONFIG =
         "<replicated-cache name=\"nyc-cache-%s\">" +
               "     <backups>" +
               "        <backup site=\"LON\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "     <backup-for remote-cache=\"lon-cache-%s\" remote-site=\"LON\" />" +
               "</replicated-cache>";

   public static final int NR_KEYS = 30;
   public static final int MAX_COUNT_KEYS = 10;
   public static final String LON_CACHE_OFF_HEAP =
         "<distributed-cache name=\"%s\" owners=\"2\" mode=\"ASYNC\" remote-timeout=\"25000\" statistics=\"true\">" +
               "        <backups>" +
               "            <backup site=\"" + NYC + "\" strategy=\"ASYNC\" timeout=\"30000\">" +
               "                <take-offline after-failures=\"-1\" min-wait=\"60000\"/>" +
               "            </backup>" +
               "        </backups>" +
               "        <memory storage=\"OFF_HEAP\" max-count=\"" + MAX_COUNT_KEYS + "\" when-full=\"REMOVE\"/>" +
               "        <persistence passivation=\"true\">" +
               "     <file-store shared=\"false\" preload=\"true\" purge=\"false\" />" +
               "  </persistence>" +
               "</distributed-cache>";

   static InfinispanServerExtensionBuilder lonServerRule = InfinispanServerExtensionBuilder
         .config("configuration/XSiteServerTest.xml")
         .runMode(ServerRunMode.CONTAINER)
         .numServers(NUM_SERVERS);

   static InfinispanServerExtensionBuilder nycServerRule = InfinispanServerExtensionBuilder
         .config("configuration/XSiteServerTest.xml")
         .runMode(ServerRunMode.CONTAINER)
         .numServers(NUM_SERVERS);

   @RegisterExtension
   public static final InfinispanXSiteServerExtension SERVERS = new InfinispanXSiteServerExtensionBuilder()
            .addSite(LON, lonServerRule)
            .addSite(NYC, nycServerRule)
            .build();
}
