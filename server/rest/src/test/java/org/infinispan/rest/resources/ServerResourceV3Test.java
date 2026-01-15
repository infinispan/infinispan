package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Version;
import org.infinispan.server.core.MockProtocolServer;
import org.testng.annotations.Test;

/**
 * Tests for REST v3 Server API endpoints.
 *
 * Note: This test class verifies that v3 endpoints work correctly when called directly.
 *
 * @since 16.0
 */
@Test(groups = "functional", testName = "rest.ServerResourceV3Test")
public class ServerResourceV3Test extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new ServerResourceV3Test().withSecurity(false).browser(false),
            new ServerResourceV3Test().withSecurity(false).browser(true),
            new ServerResourceV3Test().withSecurity(true).browser(false),
            new ServerResourceV3Test().withSecurity(true).browser(true),
      };
   }

   @Test
   public void testServerInfo() {
      RestResponse response = join(client.raw().get("/rest/v3/server", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();
      assertThat(response).containsReturnedText(Version.printVersion());
   }

   @Test
   public void testServerConnectorNames() {
      RestResponse response = join(adminClient.raw().get("/rest/v3/server/connectors", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();
      assertThat(response).containsReturnedText("DummyProtocol");
   }

   @Test
   public void testServerConnectorDetail() {
      RestResponse response = join(adminClient.raw().get("/rest/v3/server/connectors/DummyProtocol", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();
      String body = response.body();
      Json jsonNode = Json.read(body);
      assertEquals("DummyProtocol", jsonNode.at("name").asString());
      assertEquals(MockProtocolServer.DEFAULT_CACHE_NAME, jsonNode.at("default-cache").asString());
      assertTrue(jsonNode.at("enabled").asBoolean());
      assertTrue(jsonNode.at("ip-filter-rules").asJsonList().isEmpty());
      assertEquals("localhost", jsonNode.at("host").asString());
      assertTrue(jsonNode.has("port"));
      assertTrue(jsonNode.has("local-connections"));
      assertTrue(jsonNode.has("global-connections"));
      assertTrue(jsonNode.has("io-threads"));
      assertTrue(jsonNode.has("pending-tasks"));
      assertTrue(jsonNode.has("total-bytes-read"));
      assertTrue(jsonNode.has("total-bytes-written"));
      assertTrue(jsonNode.has("send-buffer-size"));
      assertTrue(jsonNode.has("receive-buffer-size"));
   }

   @Test
   public void testServerReport() {
      RestResponse response = join(adminClient.raw().get("/rest/v3/server/report"));
      assertThat(response).isOk();
      assertThat(response).hasContentType("application/gzip");
   }

   @Test
   public void testIgnoreCache() {
      // List ignored caches (should be empty initially)
      RestResponse response = join(adminClient.raw().get("/rest/v3/server/ignored-caches", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();
      assertThat(response).hasReturnedText("[]");

      // Try to ignore non-existent cache (should fail)
      response = join(adminClient.raw().post("/rest/v3/server/ignored-caches/one-cache"));
      assertThat(response).isNotFound();

      // Ignore existing cache (should succeed)
      response = join(adminClient.raw().post("/rest/v3/server/ignored-caches/defaultcache"));
      assertThat(response).isOk();

      // Verify cache is in ignored list
      response = join(adminClient.raw().get("/rest/v3/server/ignored-caches", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();
      assertThat(response).hasReturnedText("[\"defaultcache\"]");

      // Try to unignore non-existent cache (should fail)
      response = join(adminClient.raw().delete("/rest/v3/server/ignored-caches/one-cache"));
      assertThat(response).isNotFound();

      // Unignore the cache (should succeed)
      response = join(adminClient.raw().delete("/rest/v3/server/ignored-caches/defaultcache"));
      assertThat(response).isOk();

      // Verify ignored list is empty again
      response = join(adminClient.raw().get("/rest/v3/server/ignored-caches", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();
      assertThat(response).hasReturnedText("[]");
   }

   @Test
   public void testOpenAPIIncludesServerV3() {
      RestResponse response = join(client.raw().get("/rest/v3/openapi", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();

      String body = response.body();
      Json spec = Json.read(body);

      // Verify OpenAPI version
      assertEquals("3.1.1", spec.at("openapi").asString());

      // Verify paths exist
      Json paths = spec.at("paths");
      assertTrue(paths.has("/rest/v3/server"));
      assertTrue(paths.has("/rest/v3/server/config"));
      assertTrue(paths.has("/rest/v3/server/connectors"));
      assertTrue(paths.has("/rest/v3/server/ignored-caches"));

      // Verify operationIds
      Json serverInfoOp = paths.at("/rest/v3/server").at("get");
      assertEquals("getServerInfo", serverInfoOp.at("operationId").asString());

      Json connectorOp = paths.at("/rest/v3/server/connectors").at("get");
      assertEquals("listConnectors", connectorOp.at("operationId").asString());
   }
}
