package org.infinispan.server.functional;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.stream.Collectors;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.AeshTestConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * @since 13.0
 */
public class RollingUpgradeDynamicStoreCliIT extends RollingUpgradeDynamicStoreIT {

   private static Path workingDir;
   private static Properties properties;

   private static String configTemplateJson;
   private static final String REMOTE_STORE_CFG_FILE = "remote-store.json";

   @BeforeAll
   public static void setup() {
      workingDir = Path.of(CommonsTestingUtil.tmpDirectory(RollingUpgradeDynamicStoreCliIT.class));
      Util.recursiveFileRemove(workingDir);
      properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.toAbsolutePath());
      try (InputStream is = RollingUpgradeDynamicStoreCliIT.class.getResourceAsStream("/cli/" + REMOTE_STORE_CFG_FILE)) {
         assert is != null;
         try (InputStreamReader isr = new InputStreamReader(is)) {
            BufferedReader reader = new BufferedReader(isr);
            configTemplateJson = reader.lines().collect(Collectors.joining(System.lineSeparator()));
         }
         Files.createDirectories(workingDir);
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   @AfterAll
   public static void teardown() {
      Util.recursiveFileRemove(workingDir);
   }

   @Override
   protected void connectTargetCluster(String cacheName) {
      Path cacheConfig = workingDir.resolve(cacheName);
      try {
         String cfg = configTemplateJson;
         cfg = cfg.replace("127.0.0.1", source.driver.getServerAddress(0).getHostAddress());
         cfg = cfg.replace("11222", Integer.toString(source.getSinglePort(0)));
         cfg = cfg.replace("cache-name", cacheName);
         Files.writeString(cacheConfig, cfg);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), properties);
         connectToCluster(terminal, target);
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.send("migrate cluster connect --file=" + cacheConfig + " --cache=" + cacheName);
         terminal.clear();
         terminal.send("migrate cluster source-connection --cache=" + cacheName);
         terminal.assertContains("remote-store");
      }
   }

   @Override
   protected void assertSourceConnected(String cacheName) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), properties);
         connectToCluster(terminal, target);
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.send("migrate cluster source-connection --cache=" + cacheName);
         terminal.assertContains("remote-store");
      }
   }

   @Override
   protected void assertSourceDisconnected(String cacheName) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), properties);
         connectToCluster(terminal, target);
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.send("migrate cluster source-connection --cache=" + cacheName);
         terminal.assertContains("Not Found");
      }
   }

   @Override
   protected void doRollingUpgrade(String cacheName, RestClient client) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), properties);
         connectToCluster(terminal, target);
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.send("migrate cluster synchronize --cache=" + cacheName);
      }
   }

   @Override
   protected void disconnectSource(String cacheName, RestClient client) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), properties);
         connectToCluster(terminal, target);
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.send("migrate cluster disconnect --cache=" + cacheName);
      }
   }

   private void connectToCluster(AeshTestConnection terminal, Cluster cluster) {
      terminal.send("connect " + cluster.driver.getServerAddress(0).getHostAddress() + ":" + cluster.getSinglePort(0));
   }
}
