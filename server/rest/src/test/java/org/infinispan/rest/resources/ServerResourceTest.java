package org.infinispan.rest.resources;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Version;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.server.core.MockProtocolServer;
import org.testng.annotations.Test;

/**
 * @since 14.0
 */
@Test(groups = "functional", testName = "rest.ServerResourceTest")
public class ServerResourceTest extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new ServerResourceTest().withSecurity(false),
            new ServerResourceTest().withSecurity(true),
      };
   }

   @Test
   public void testServerInfo() {
      CompletionStage<RestResponse> response = client.server().info();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).containsReturnedText(Version.printVersion());
   }

   @Test
   public void testServerConnectorNames() {
      CompletionStage<RestResponse> response = adminClient.server().connectorNames();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).containsReturnedText("DummyProtocol");
   }

   @Test
   public void testServerConnectorDetail() {
      CompletionStage<RestResponse> response = adminClient.server().connector("DummyProtocol");
      ResponseAssertion.assertThat(response).isOk();
      String body = join(response).getBody();
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
      CompletionStage<RestResponse> response = adminClient.server().report();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/gzip");
   }
}
