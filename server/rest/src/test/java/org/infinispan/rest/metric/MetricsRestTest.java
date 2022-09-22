package org.infinispan.rest.metric;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "rest.metric.MetricsRestTest")
public class MetricsRestTest extends SingleCacheManagerTest {

   private RestServerHelper restServer;
   private RestClient restClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager();

      restServer = new RestServerHelper(cacheManager);
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      restClient = RestClient.forConfiguration(new RestClientConfigurationBuilder().addServer()
            .host(restServer.getHost()).port(restServer.getPort())
            .build());

      return cacheManager;
   }

   @Test
   public void smokeTest() {
      String response = restClient.metrics().metrics().toCompletableFuture().join().getBody();
      assertThat(response).contains("#");
   }

   @Override
   protected void teardown() {
      try {
         restClient.close();
      } catch (IOException ex) {
         // ignore it
      } finally {
         try {
            restServer.stop();
         } finally {
            super.teardown();
         }
      }
   }
}
