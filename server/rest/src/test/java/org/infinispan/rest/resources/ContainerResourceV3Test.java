package org.infinispan.rest.resources;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestResponse;
import org.testng.annotations.Test;

/**
 * Tests for REST v3 Container API endpoints.
 * <p>
 * This class extends ContainerResourceTest and overrides the helper methods to use
 * v3 REST endpoints, ensuring full test coverage parity with v2.
 * <p>
 * All v2 tests are inherited and executed against v3 endpoints, including SSE
 * (Server-Sent Events) streaming endpoints.
 *
 * @since 16.0
 */
@Test(groups = "functional", testName = "rest.ContainerResourceV3Test")
public class ContainerResourceV3Test extends ContainerResourceTest {

   @Override
   protected CompletionStage<RestResponse> getHealth() {
      return client.raw().get("/rest/v3/container/health");
   }

   @Override
   protected CompletionStage<RestResponse> getHealthStatus() {
      return client.raw().head("/rest/v3/container/health/status");
   }

   @Override
   protected CompletionStage<RestResponse> getCacheConfigurations(String accept) {
      return client.raw().get("/rest/v3/container/cache-configs", Map.of("Accept", accept));
   }

   @Override
   protected CompletionStage<RestResponse> getTemplates(String accept) {
      return client.raw().get("/rest/v3/container/cache-configs/templates", Map.of("Accept", accept));
   }

   @Override
   protected CompletionStage<RestResponse> getGlobalConfiguration() {
      return adminClient.raw().get("/rest/v3/container/config");
   }

   @Override
   protected CompletionStage<RestResponse> getGlobalConfigurationWithMediaType(String mediaType) {
      return adminClient.raw().get("/rest/v3/container/config", Map.of("Accept", mediaType));
   }

   @Override
   protected CompletionStage<RestResponse> getInfo() {
      return client.raw().get("/rest/v3/container");
   }

   @Override
   protected CompletionStage<RestResponse> getStats() {
      return adminClient.raw().get("/rest/v3/container/_stats");
   }

   @Override
   protected CompletionStage<RestResponse> enableRebalancing() {
      return adminClient.raw().post("/rest/v3/container/_rebalancing-enable");
   }

   @Override
   protected CompletionStage<RestResponse> disableRebalancing() {
      return adminClient.raw().post("/rest/v3/container/_rebalancing-disable");
   }

   @Override
   protected String getConfigListenerEndpoint() {
      return "/rest/v3/container/config/_listen?includeCurrentState=true";
   }
}
