package org.infinispan.server.functional;

import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;

import org.infinispan.server.cli.XSiteCliOperations;
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

   protected static final int NUM_SERVERS = 3;
   protected static final String LON_CACHE_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <replicated-cache name=\"%s\">" +
               "     <backups>" +
               "        <backup site=\"" + NYC + "\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "  </replicated-cache>" +
               "</cache-container></infinispan>";

   protected static final String NYC_CACHE_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <replicated-cache name=\"%s\">" +
               "     <backups>" +
               "        <backup site=\"" + LON + "\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "  </replicated-cache>" +
               "</cache-container></infinispan>";

   protected static final String LON_CACHE_CUSTOM_NAME_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <replicated-cache name=\"lon-cache\">" +
               "     <backups>" +
               "        <backup site=\"" + NYC + "\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "     <backup-for remote-cache=\"nyc-cache\" remote-site=\"NYC\" />" +
               "  </replicated-cache>" +
               "</cache-container></infinispan>";

   protected static final String NYC_CACHE_CUSTOM_NAME_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <replicated-cache name=\"nyc-cache\">" +
               "     <backups>" +
               "        <backup site=\"" + LON + "\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "     <backup-for remote-cache=\"lon-cache\" remote-site=\"LON\" />" +
               "  </replicated-cache>" +
               "</cache-container></infinispan>";

   protected static final String LON_CACHE_OFF_HEAP =
         "<infinispan><cache-container statistics=\"true\">" +
               "<distributed-cache name=\"%s\" owners=\"2\" mode=\"ASYNC\" remote-timeout=\"25000\" statistics=\"true\">" +
               "        <backups>" +
               "            <backup site=\"" + NYC + "\" strategy=\"ASYNC\" timeout=\"30000\">" +
               "                <take-offline after-failures=\"-1\" min-wait=\"60000\"/>" +
               "            </backup>" +
               "        </backups>" +
               "        <memory storage=\"OFF_HEAP\" max-count=\"100\" when-full=\"REMOVE\"/>" +
               "        <persistence passivation=\"true\">" +
               "     <file-store shared=\"false\" preload=\"true\" purge=\"false\" />" +
               "  </persistence>" +
               "  </distributed-cache>" +
               "</cache-container></infinispan>";

   static InfinispanServerRuleBuilder lonServerRule = InfinispanServerRuleBuilder
         .config("configuration/XSiteServerTest.xml")
         .numServers(NUM_SERVERS);

   static InfinispanServerRuleBuilder nycServerRule = InfinispanServerRuleBuilder
         .config("configuration/XSiteServerTest.xml")
         .numServers(NUM_SERVERS);

   @ClassRule
   public static final InfinispanXSiteServerRule SERVERS = new InfinispanXSiteServerRuleBuilder()
            .addSite(LON, lonServerRule)
            .addSite(NYC, nycServerRule)
            .build();
}
