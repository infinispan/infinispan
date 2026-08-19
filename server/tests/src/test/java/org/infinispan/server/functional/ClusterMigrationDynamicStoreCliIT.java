package org.infinispan.server.functional;

import static io.smallrye.common.constraint.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestClient;
import org.infinispan.server.test.core.CliConnection;
import org.junit.jupiter.api.BeforeAll;

/**
 * @since 13.0
 */
public class ClusterMigrationDynamicStoreCliIT extends ClusterMigrationDynamicStoreIT {
   private static String configTemplateJson;
   private static final String REMOTE_STORE_CFG_FILE = "remote-store.json";

   @BeforeAll
   public static void setup() {
      try (InputStream is = ClusterMigrationDynamicStoreCliIT.class.getResourceAsStream("/cli/" + REMOTE_STORE_CFG_FILE)) {
         assertNotNull(is);
         try (InputStreamReader isr = new InputStreamReader(is)) {
            BufferedReader reader = new BufferedReader(isr);
            configTemplateJson = reader.lines().collect(Collectors.joining(System.lineSeparator()));
         }

      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   protected void connectTargetCluster(String cacheName) {
      Path cacheConfig = source.driver.getRootDir().toPath().resolve(cacheName);
      try {
         String cfg = configTemplateJson;
         cfg = cfg.replace("127.0.0.1", source.driver.getServerAddress(0).getHostAddress());
         cfg = cfg.replace("11222", Integer.toString(source.getSinglePort(0)));
         cfg = cfg.replace("cache-name", cacheName);
         Files.writeString(cacheConfig, cfg);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      CliConnection connection = target.cli().connect();
      connection.send("migrate cluster connect --file=" + cacheConfig + " --cache=" + cacheName);
      connection.clear();
      connection.send("migrate cluster source-connection --cache=" + cacheName);
      connection.assertContains("remote-store");

   }

   @Override
   protected void assertSourceConnected(String cacheName) {
      CliConnection connection = target.cli().connect();
      connection.send("migrate cluster source-connection --cache=" + cacheName);
      connection.assertContains("remote-store");
   }

   @Override
   protected void assertSourceDisconnected(String cacheName) {
      CliConnection connection = target.cli().connect();
      connection.send("migrate cluster source-connection --cache=" + cacheName);
      connection.assertContains("Not Found");
   }

   @Override
   protected void migrate(String cacheName, RestClient client) {
      CliConnection connection = target.cli().connect();
      connection.send("migrate cluster synchronize --cache=" + cacheName);
   }

   @Override
   protected void disconnectSource(String cacheName, RestClient client) {
      CliConnection connection = target.cli().connect();
      connection.send("migrate cluster disconnect --cache=" + cacheName);
   }
}
