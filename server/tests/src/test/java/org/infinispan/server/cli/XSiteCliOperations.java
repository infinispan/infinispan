package org.infinispan.server.cli;

import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aesh.terminal.utils.Config;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.server.functional.XSiteIT;
import org.infinispan.server.test.api.TestClientXSiteDriver;
import org.infinispan.server.test.core.CliConnection;
import org.infinispan.server.test.jupiter.InfinispanServer;
import org.junit.jupiter.api.Test;

/**
 * CLI test for 'site' command
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public class XSiteCliOperations {

   @InfinispanServer(XSiteIT.class)
   public static TestClientXSiteDriver SERVERS;

   @Test
   public void testSiteView() {
      CliConnection connection = SERVERS.cli(LON).connect();

      connection.send("site name");
      connection.assertContains(LON);
      connection.clear();

      connection.send("site view");
      connection.assertContains(LON);
      connection.assertContains(NYC);
      connection.clear();

      connection = SERVERS.cli(NYC).connect();

      connection.send("site name");
      connection.assertContains(NYC);
      connection.clear();

      connection.send("site view");
      connection.assertContains(LON);
      connection.assertContains(NYC);
      connection.clear();
   }

   @Test
   public void testStateTransferModeCli() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.clustering().sites().addBackup()
            .site(NYC)
            .strategy(BackupConfiguration.BackupStrategy.ASYNC)
            .stateTransfer().mode(XSiteStateTransferMode.AUTO);

      SERVERS.hotrod(LON).createRemoteCacheManager()
            .administration()
            .createCache("st-mode", builder.build());

      CliConnection connection = SERVERS.cli(LON).connect();

      //make sure --site is required
      connection.send("site state-transfer-mode get");
      connection.assertContains("Option: --site is required for this command.");
      connection.clear();

      //check command invoked in the wrong context
      connection.send("site state-transfer-mode get --site=" + NYC);
      connection.assertContains("Command invoked from the wrong context");
      connection.clear();

      //check non xsite cache
      connection.send("cd caches/" + SCRIPT_CACHE_NAME);
      connection.clear();
      connection.send("site state-transfer-mode get --site=" + NYC);
      connection.assertContains("Not Found: Cache '" + SCRIPT_CACHE_NAME + "' does not have backup sites.");
      connection.clear();

      //check if --cache overrides the context
      connection.send("site state-transfer-mode get --cache=st-mode --site=" + NYC);
      connection.assertContains("AUTO");
      connection.clear();

      //check if --cache is not required
      connection.send("cd ../st-mode");
      connection.clear();
      connection.send("site state-transfer-mode get --site=" + NYC);
      connection.assertContains("AUTO");
      connection.clear();

      //check invalid site
      connection.send("site state-transfer-mode get --site=NOT_A_SITE");
      connection.assertContains("Not Found: Cache 'st-mode' does not backup to site 'NOT_A_SITE'");
      connection.clear();

      //check set!
      connection.send("site state-transfer-mode set --mode=MANUAL --site=" + NYC);
      connection.clear();
      connection.send("site state-transfer-mode get --site=" + NYC);
      connection.assertContains("MANUAL");
      connection.clear();

      //check invalid mode
      connection.send("site state-transfer-mode set --mode=ABC --site=" + NYC);
      connection.assertContains("No enum constant org.infinispan.client.rest.XSiteStateTransferMode.ABC");
      connection.clear();
   }

   @Test
   public void testRelayNodeInfo() {
      CliConnection connection = SERVERS.cli(LON).connect();

      connection.send("site is-relay-node");
      connection.assertContains("true");
      connection.clear();

      // max_site_master is 100 so the relay-nodes is the same as cluster_members
      // method has side effects, invoke before "site relay-nodes"
      List<String> view = extractView(connection);

      connection.send("site relay-nodes");

      view.forEach(connection::assertContains);

      connection.clear();
   }

   private void connect(CliConnection terminal, String site) {
      // connect
      terminal.send("connect " + SERVERS.hostAndPort(site));
      terminal.assertContains("//containers/default]>");
      terminal.clear();
   }

   private void disconnect(CliConnection terminal) {
      // connect
      terminal.send("disconnect");
      terminal.clear();
   }

   private static List<String> extractView(CliConnection terminal) {
      terminal.send("describe");
      // make sure the command succeed
      terminal.assertContains("//containers/default");
      String allOutput = terminal.getOutputBuffer();
      Pattern pattern = Pattern.compile("^\\s*\"cluster_members\"\\s*:\\s*\\[\\s+(.*)\\s+],\\s*$");
      for (String line : allOutput.split(Config.getLineSeparator())) {
         line = line.trim();
         Matcher matcher = pattern.matcher(line);
         if (matcher.matches()) {
            terminal.clear();
            return Stream.of(matcher.group(1).split(","))
                  .map(s -> s.replaceAll("[\\[\\]\"]", ""))
                  .collect(Collectors.toList());
         }
      }
      terminal.clear();
      throw new IllegalStateException("Unable to find 'cluster_members' in:\n" + allOutput);
   }
}
