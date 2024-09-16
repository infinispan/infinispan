package org.infinispan.server.resilience;

import static org.infinispan.server.test.core.Common.assertStatus;

import java.util.stream.IntStream;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Start cluster (0..numServers) redeploy after upgrade. Rolling upgrades always occur in the order numServers...0 and
 * the node with index numServers - 1 does not restart until node numServers has completed successfully.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class StatefulSetRollingUpgradeIT {

   private static final int NUM_ROLLING_UPGRADES = 4;

   @ParameterizedTest
   @ValueSource(ints = {2, 3, 4, 5})
   public void testStatefulSetRollingUpgrade(int numServers) {
      TestServer server = new TestServer(
            InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
                  .numServers(numServers)
                  .runMode(ServerRunMode.CONTAINER)
                  .parallelStartup(false)
                  .createServerTestConfiguration()
      );
      String testName = StatefulSetRollingUpgradeIT.class.getSimpleName();
      server.initServerDriver();
      server.getDriver().prepare(testName);
      server.getDriver().start(testName);

      TestClient client = new TestClient(server);
      try {
         client.initResources();
         client.setMethodName(testName + "-" + numServers);

         IntStream.range(0, numServers).forEach(i -> assertLiveness(client, i));
         InfinispanServerDriver serverDriver = server.getDriver();

         // ISPN-13997 Ensure that Memory max-size is represented in original format in caches.xml
         String cacheConfig = "<replicated-cache name=\"cache01\"><memory storage=\"OFF_HEAP\" max-size=\"200MB\"/><transaction transaction-manager-lookup=\"org.infinispan.transaction.lookup.GenericTransactionManagerLookup\"/></replicated-cache>";
         RestEntity entity = RestEntity.create(MediaType.APPLICATION_XML, cacheConfig);
         assertStatus(HttpResponseStatus.OK.code(), client.rest().get().cache("cache01").createWithConfiguration(entity));
         for (int i = 0; i < NUM_ROLLING_UPGRADES; i++) {
            for (int j = numServers - 1; j > -1; j--) {
               serverDriver.stop(j);
               serverDriver.restart(j);
               assertLiveness(client, j);
            }
         }
      } finally {
         client.clearResources();
         server.stopServerDriver(testName);
      }
   }

   private void assertLiveness(TestClient client, int server) {
      RestClient rest = client.rest().get(server);
      assertStatus(HttpResponseStatus.OK.code(), rest.container().healthStatus());
   }
}
