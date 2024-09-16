package org.infinispan.server.persistence;

import static org.infinispan.server.persistence.PersistenceIT.getJavaArchive;
import static org.infinispan.server.persistence.PersistenceIT.getJdbcDrivers;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_CONTAINER_DATABASE_TYPES;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Eventually;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.persistence.ContainerDatabase;
import org.infinispan.server.test.core.persistence.DatabaseServerListener;
import org.infinispan.server.test.core.tags.Database;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Integration tests for JDBC_PING2 to ensure that the various failure scenarios work as expected with the Infinispan server.
 * Tests not reliant on the Infinispan server should be added to {@link JGroupsJdbcPing2IT}.
 */
@Database
@DisabledIf("customDatabaseTypes")
public class JdbcPing2IT {

   static final DatabaseServerListener DATABASE_LISTENER = new DatabaseServerListener("mysql");

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config(System.getProperty(JdbcPing2IT.class.getName(), "configuration/JdbcPingTest.xml"))
               .numServers(2)
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(getJdbcDrivers())
               .artifacts(getJavaArchive())
               .addListener(DATABASE_LISTENER)
               .property(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
               .build();

   public static class DatabaseProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
         return Arrays.stream(DATABASE_LISTENER.getDatabaseTypes())
               .map(DATABASE_LISTENER::getDatabase)
               .filter(ContainerDatabase.class::isInstance)
               .map(Arguments::of);
      }
   }

   static boolean customDatabaseTypes() {
      // Ignore execution for all types other than mysql as this test is only concerned with Infinispan server and
      // JDBC_PING2's ability to recover in the scenario. DB specific tests are located in JGroupsJdbcPing2IT.
      // Necessary to ignore the test when explicitly configuring the 'org.infinispan.test.database.types' property.
      String property = System.getProperty(INFINISPAN_TEST_CONTAINER_DATABASE_TYPES);
      return property != null && !"mysql".equals(property);
   }

   @ParameterizedTest
   @ArgumentsSource(DatabaseProvider.class)
   public void testZombieProcess(ContainerDatabase db) {
      // Create cluster of 2
      // Gracefully kill 0
      // Kill -9 1
      // Start new server
      // Process hangs
      var timeout = TimeUnit.MINUTES.toMillis(20);
      SERVERS.getServerDriver().stop(0);
      Eventually.eventually(assertClusterMembersSize(1, 1), timeout);
      SERVERS.getServerDriver().kill(1);
      SERVERS.getServerDriver().restart(0);
      Eventually.eventually(assertClusterMembersSize(0, 1), timeout);
      SERVERS.getServerDriver().restart(1);
      Eventually.eventually(assertClusterMembersSize(0, 2), timeout);
      Eventually.eventually(assertClusterMembersSize(1, 2), timeout);
   }

   private Eventually.Condition assertClusterMembersSize(int server, int expectedMembers) {
      RestClient client = SERVERS.rest().get(server);
      try (RestResponse info = sync(client.container().info())) {
         assertEquals(200, info.status());
         Json json = Json.read(info.body());
         return () -> json.at("cluster_members").asJsonList().size() == expectedMembers;
      }
   }
}
