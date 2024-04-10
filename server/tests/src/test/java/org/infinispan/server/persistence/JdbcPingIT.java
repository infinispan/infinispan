package org.infinispan.server.persistence;

import static org.infinispan.server.persistence.PersistenceIT.getJavaArchive;
import static org.infinispan.server.persistence.PersistenceIT.getJdbcDrivers;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Persistence;
import org.infinispan.server.test.core.persistence.DatabaseServerListener;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Category(Persistence.class)
public class JdbcPingIT {

   static final DatabaseServerListener DATABASE_LISTENER = new DatabaseServerListener("mysql");

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config(System.getProperty(JdbcPingIT.class.getName(), "configuration/JdbcPingTest.xml"))
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(getJdbcDrivers())
               .artifacts(getJavaArchive())
               .addListener(DATABASE_LISTENER)
               .property(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
               .build();

   @Test
   public void testJDBCPing() {
      RestClient client = SERVERS.rest().get();
      try (RestResponse info = sync(client.container().info())) {
         assertEquals(200, info.status());
         Json json = Json.read(info.body());
         assertEquals(1, json.at("cluster_members").asJsonList().size());
      }
   }
}
