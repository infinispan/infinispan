package org.infinispan.server.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.infinispan.cli.commands.Mcp;
import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Integration test for the MCP stdio transport bridge CLI command.
 * Starts a server with the MCP feature enabled, then exercises the bridge
 * by sending JSON-RPC messages through byte array streams.
 *
 * @since 16.2
 */
public class McpCliIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .property("org.infinispan.feature.mcp", "true")
               .build();

   @Test
   @Timeout(30)
   public void testInitializeAndToolsList() throws Exception {
      RestClient client = SERVERS.rest().create();

      String input = "{\"jsonrpc\":\"2.0\",\"method\":\"initialize\","
            + "\"params\":{\"protocolVersion\":\"2025-03-26\",\"capabilities\":{}},"
            + "\"id\":1}\n"
            + "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\",\"params\":{},\"id\":2}\n";

      ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      Mcp.runStdioBridge(client, in, new PrintStream(out, true, StandardCharsets.UTF_8));

      String[] responses = out.toString(StandardCharsets.UTF_8).trim().split("\n");
      assertEquals(2, responses.length, "Should receive two responses");

      // Verify initialize response
      Json initJson = Json.read(responses[0]);
      assertEquals("2.0", initJson.at("jsonrpc").asString());
      assertEquals(1, initJson.at("id").asInteger());

      Json result = initJson.at("result");
      assertNotNull(result);
      assertEquals("2025-03-26", result.at("protocolVersion").asString());
      assertTrue(result.has("serverInfo"));
      assertTrue(result.has("capabilities"));

      // Verify tools/list response
      Json toolsJson = Json.read(responses[1]);
      assertEquals("2.0", toolsJson.at("jsonrpc").asString());
      assertEquals(2, toolsJson.at("id").asInteger());

      Json tools = toolsJson.at("result").at("tools");
      assertTrue(tools.isArray());
      int toolCount = tools.asList().size();
      assertTrue(toolCount > 0, "Should have at least one tool");

      boolean hasGetCacheNames = false;
      for (int i = 0; i < toolCount; i++) {
         if ("getCacheNames".equals(tools.at(i).at("name").asString())) {
            hasGetCacheNames = true;
            break;
         }
      }
      assertTrue(hasGetCacheNames, "Should have getCacheNames tool");
   }

   @Test
   @Timeout(30)
   public void testToolCall() throws Exception {
      RestClient client = SERVERS.rest().create();

      String input = "{\"jsonrpc\":\"2.0\",\"method\":\"initialize\","
            + "\"params\":{\"protocolVersion\":\"2025-03-26\",\"capabilities\":{}},"
            + "\"id\":1}\n"
            + "{\"jsonrpc\":\"2.0\",\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"getCacheNames\",\"arguments\":{}},"
            + "\"id\":2}\n";

      ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      Mcp.runStdioBridge(client, in, new PrintStream(out, true, StandardCharsets.UTF_8));

      String[] responses = out.toString(StandardCharsets.UTF_8).trim().split("\n");
      assertEquals(2, responses.length, "Should receive two responses");

      // Skip initialize response (index 0), verify tool call response
      Json toolJson = Json.read(responses[1]);
      assertEquals("2.0", toolJson.at("jsonrpc").asString());
      assertEquals(2, toolJson.at("id").asInteger());

      Json content = toolJson.at("result").at("content");
      assertTrue(content.isArray(), "Tool result should have content array");
      assertTrue(content.asList().size() > 0, "Content should not be empty");

      Json firstContent = content.at(0);
      assertEquals("text", firstContent.at("type").asString());
   }

   @Test
   @Timeout(30)
   public void testInvalidJsonRpc() throws Exception {
      RestClient client = SERVERS.rest().create();

      String input = "not valid json\n";

      ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      Mcp.runStdioBridge(client, in, new PrintStream(out, true, StandardCharsets.UTF_8));

      String[] responses = out.toString(StandardCharsets.UTF_8).trim().split("\n");
      assertTrue(responses.length > 0, "Should receive at least one response");

      Json errorJson = Json.read(responses[0]);
      assertTrue(errorJson.has("error") || errorJson.has("result"),
            "Response should be a valid JSON-RPC message");
   }
}
