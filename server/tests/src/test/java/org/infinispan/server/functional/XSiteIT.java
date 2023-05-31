package org.infinispan.server.functional;

import org.infinispan.server.cli.XSiteCliOperations;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanXSiteServerRule;
import org.infinispan.server.test.junit4.InfinispanXSiteServerRuleBuilder;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Cross-Site suite
 *
 * @author Pedro Ruivo
 * @author Gustavo Lira
 * @since 11.0
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
      XSiteRestMetricsOperations.class,
      XSiteHotRodCacheOperations.class,
      XSiteRestCacheOperations.class,
      XSiteCliOperations.class
})
public class XSiteIT {

   public static final String LON = "LON";
   public static final String NYC = "NYC";
   public static final int NUM_SERVERS = 3;

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

   static InfinispanServerRuleBuilder lonServerRule = InfinispanServerRuleBuilder
         .config("configuration/XSiteServerTest.xml")
         .runMode(ServerRunMode.CONTAINER)
         .numServers(NUM_SERVERS);

   static InfinispanServerRuleBuilder nycServerRule = InfinispanServerRuleBuilder
         .config("configuration/XSiteServerTest.xml")
         .runMode(ServerRunMode.CONTAINER)
         .numServers(NUM_SERVERS);

   @ClassRule
   public static final InfinispanXSiteServerRule SERVERS = new InfinispanXSiteServerRuleBuilder()
            .addSite(LON, lonServerRule)
            .addSite(NYC, nycServerRule)
            .build();
}
