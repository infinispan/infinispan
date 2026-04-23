package org.infinispan.rest.resources;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.testing.Testing.tmpDirectory;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.McpTest")
public class McpTest extends AbstractRestResourceTest {

   static {
      System.setProperty("org.infinispan.feature.mcp", "true");
   }

   private final String PERSISTENT_LOCATION = tmpDirectory(this.getClass().getName());
   private final AtomicInteger rpcId = new AtomicInteger(1);

   @Override
   public Object[] factory() {
      return new Object[]{
            new McpTest().withSecurity(false),
            new McpTest().withSecurity(true),
      };
   }

   @Override
   protected String parameters() {
      return "[security=" + security + "]";
   }

   @Override
   protected GlobalConfigurationBuilder getGlobalConfigForNode(int id) {
      GlobalConfigurationBuilder config = super.getGlobalConfigForNode(id);
      config.globalState().enable()
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .persistentLocation(Paths.get(PERSISTENT_LOCATION, Integer.toString(id)).toString());
      return config;
   }

   @Override
   protected void createCacheManagers() throws Exception {
      Util.recursiveFileRemove(PERSISTENT_LOCATION);
      super.createCacheManagers();
   }

   private Json rpcRequest(String method) {
      return Json.object()
            .set("jsonrpc", "2.0")
            .set("id", rpcId.getAndIncrement())
            .set("method", method);
   }

   private Json rpcRequest(String method, Json params) {
      return rpcRequest(method).set("params", params);
   }

   private Json toolCall(String toolName, Json arguments) {
      return rpcRequest("tools/call", Json.object()
            .set("name", toolName)
            .set("arguments", arguments));
   }

   private Json mcpCall(Json request) {
      RestResponse response = join(adminClient.raw().post("/rest/v3/mcp",
            RestEntity.create(APPLICATION_JSON, request.toString())));
      assertThat(response).isOk();
      return Json.read(response.body());
   }

   private Json callTool(String toolName, Json arguments) {
      Json request = toolCall(toolName, arguments);
      RestResponse restResponse = join(adminClient.raw().post("/rest/v3/mcp",
            RestEntity.create(APPLICATION_JSON, request.toString())));
      String body = restResponse.body();
      assertThat(restResponse).isOk();
      Json response = Json.read(body);
      Json result = response.at("result");
      assertNotNull("No result in response: " + body, result);
      assertFalse("Tool " + toolName + " returned error: " + body, result.at("isError").asBoolean());
      return result.at("content");
   }

   private String callToolText(String toolName, Json arguments) {
      Json content = callTool(toolName, arguments);
      assertTrue(content.isArray());
      return content.at(0).at("text").asString();
   }

   @Test
   public void testInitialize() {
      Json response = mcpCall(rpcRequest("initialize", Json.object()
            .set("protocolVersion", "2025-03-26")
            .set("capabilities", Json.object())
            .set("clientInfo", Json.object().set("name", "test").set("version", "1.0"))));
      Json result = response.at("result");
      assertNotNull(result);
      assertNotNull(result.at("serverInfo"));
      assertEquals("2.0", response.at("jsonrpc").asString());
   }

   @Test
   public void testToolsList() {
      Json response = mcpCall(rpcRequest("tools/list"));
      Json tools = response.at("result").at("tools");
      assertNotNull(tools);
      assertTrue(tools.isArray());
      assertFalse(tools.asJsonList().isEmpty());
      boolean foundCreateCache = false;
      boolean foundDeleteCache = false;
      boolean foundSetCacheEntries = false;
      for (Json tool : tools.asJsonList()) {
         String name = tool.at("name").asString();
         if ("createCache".equals(name)) foundCreateCache = true;
         if ("deleteCache".equals(name)) foundDeleteCache = true;
         if ("setCacheEntries".equals(name)) foundSetCacheEntries = true;
      }
      assertTrue(foundCreateCache);
      assertTrue(foundDeleteCache);
      assertTrue(foundSetCacheEntries);
   }

   @Test
   public void testResourcesList() {
      Json response = mcpCall(rpcRequest("resources/list"));
      Json resources = response.at("result").at("resources");
      assertNotNull(resources);
      assertTrue(resources.isArray());
      assertFalse(resources.asJsonList().isEmpty());
   }

   @Test
   public void testGetCacheNames() {
      String text = callToolText("getCacheNames", Json.object());
      assertNotNull(text);
   }

   @Test
   public void testCreateAndDeleteCache() {
      callTool("createCache", Json.object().set("cacheName", "mcpTestCache"));

      String names = callToolText("getCacheNames", Json.object());
      assertTrue(names.contains("mcpTestCache"));

      callTool("deleteCache", Json.object().set("cacheName", "mcpTestCache"));

      names = callToolText("getCacheNames", Json.object());
      assertFalse(names.contains("mcpTestCache"));
   }

   @Test
   public void testCreateCacheWithConfiguration() {
      String config = Json.object()
            .set("distributed-cache", Json.object()
                  .set("mode", "SYNC")
                  .set("statistics", true)
                  .set("encoding", Json.object()
                        .set("media-type", "application/x-protostream"))
            ).toString();
      callTool("createCache", Json.object()
            .set("cacheName", "mcpConfiguredCache")
            .set("configuration", config));

      String names = callToolText("getCacheNames", Json.object());
      assertTrue(names.contains("mcpConfiguredCache"));

      String configResult = callToolText("getCacheConfiguration", Json.object()
            .set("cacheName", "mcpConfiguredCache"));
      assertTrue(configResult.contains("statistics"));

      callTool("deleteCache", Json.object().set("cacheName", "mcpConfiguredCache"));
   }

   @Test
   public void testSetGetDeleteCacheEntry() {
      callTool("createCache", Json.object().set("cacheName", "mcpEntryCache"));
      try {
         callTool("setCacheEntry", Json.object()
               .set("cacheName", "mcpEntryCache")
               .set("key", "k1")
               .set("value", "v1"));

         String value = callToolText("getCacheEntry", Json.object()
               .set("cacheName", "mcpEntryCache")
               .set("key", "k1"));
         assertEquals("v1", value);

         callTool("deleteCacheEntry", Json.object()
               .set("cacheName", "mcpEntryCache")
               .set("key", "k1"));

         Json afterDelete = mcpCall(toolCall("getCacheEntry", Json.object()
               .set("cacheName", "mcpEntryCache")
               .set("key", "k1")));
         assertTrue(afterDelete.at("result").at("isError").asBoolean());
      } finally {
         callTool("deleteCache", Json.object().set("cacheName", "mcpEntryCache"));
      }
   }

   @Test
   public void testSetCacheEntries() {
      callTool("createCache", Json.object().set("cacheName", "mcpBulkCache"));
      try {
         Json entries = Json.array()
               .add(Json.object().set("key", "b1").set("value", "val1"))
               .add(Json.object().set("key", "b2").set("value", "val2"))
               .add(Json.object().set("key", "b3").set("value", "val3"));
         String result = callToolText("setCacheEntries", Json.object()
               .set("cacheName", "mcpBulkCache")
               .set("entries", entries.toString()));
         assertTrue(result.contains("3"));

         assertEquals("val1", callToolText("getCacheEntry", Json.object()
               .set("cacheName", "mcpBulkCache").set("key", "b1")));
         assertEquals("val2", callToolText("getCacheEntry", Json.object()
               .set("cacheName", "mcpBulkCache").set("key", "b2")));
         assertEquals("val3", callToolText("getCacheEntry", Json.object()
               .set("cacheName", "mcpBulkCache").set("key", "b3")));
      } finally {
         callTool("deleteCache", Json.object().set("cacheName", "mcpBulkCache"));
      }
   }

   @Test
   public void testGetCacheConfiguration() {
      callTool("createCache", Json.object().set("cacheName", "mcpConfigCache"));
      try {
         String config = callToolText("getCacheConfiguration", Json.object()
               .set("cacheName", "mcpConfigCache"));
         assertNotNull(config);
         assertTrue(config.contains("distributed-cache"));
      } finally {
         callTool("deleteCache", Json.object().set("cacheName", "mcpConfigCache"));
      }
   }

   @Test
   public void testGetCacheStats() {
      callTool("createCache", Json.object().set("cacheName", "mcpStatsCache"));
      try {
         String stats = callToolText("getCacheStats", Json.object()
               .set("cacheName", "mcpStatsCache"));
         assertNotNull(stats);
      } finally {
         callTool("deleteCache", Json.object().set("cacheName", "mcpStatsCache"));
      }
   }

   @Test
   public void testRegisterAndGetSchemas() {
      String schema = """
            message McpTestPerson {
               optional string name = 1;
               optional int32 age = 2;
            }
            """;
      String result = callToolText("registerSchema", Json.object()
            .set("schemaName", "mcptest.proto")
            .set("schema", schema));
      assertTrue(result.contains("registered successfully"));

      String schemas = callToolText("getSchemas", Json.object());
      assertTrue(schemas.contains("mcptest.proto"));
   }

   @Test
   public void testQueryCache() {
      String schema = """
            /**
             * @Indexed
             */
            message McpQueryEntry {
               /**
                * @Field(index=Index.YES, store=Store.YES)
                */
               optional string name = 1;
            }
            """;
      callToolText("registerSchema", Json.object()
            .set("schemaName", "mcpquery.proto")
            .set("schema", schema));

      String config = Json.object()
            .set("distributed-cache", Json.object()
                  .set("mode", "SYNC")
                  .set("encoding", Json.object()
                        .set("media-type", "application/x-protostream"))
                  .set("indexing", Json.object()
                        .set("enabled", true)
                        .set("indexed-entities", Json.array("McpQueryEntry"))
                  )
            ).toString();
      callTool("createCache", Json.object()
            .set("cacheName", "mcpQueryCache")
            .set("configuration", config));
      try {
         callTool("setCacheEntry", Json.object()
               .set("cacheName", "mcpQueryCache")
               .set("key", "1")
               .set("value", "{\"_type\":\"McpQueryEntry\",\"name\":\"Alice\"}")
               .set("mediaType", "application/json"));
         callTool("setCacheEntry", Json.object()
               .set("cacheName", "mcpQueryCache")
               .set("key", "2")
               .set("value", "{\"_type\":\"McpQueryEntry\",\"name\":\"Bob\"}")
               .set("mediaType", "application/json"));

         String result = callToolText("queryCache", Json.object()
               .set("cacheName", "mcpQueryCache")
               .set("query", "FROM McpQueryEntry WHERE name = 'Alice'"));
         assertTrue(result.contains("Alice"));
         assertFalse(result.contains("Bob"));
      } finally {
         callTool("deleteCache", Json.object().set("cacheName", "mcpQueryCache"));
      }
   }

   @Test
   public void testCounterOperations() {
      callTool("createCounter", Json.object()
            .set("counterName", "mcpCounter")
            .set("type", "UNBOUNDED_STRONG")
            .set("initialValue", 0));
      try {
         String names = callToolText("getCounterNames", Json.object());
         assertTrue(names.contains("mcpCounter"));

         callTool("increment", Json.object().set("counterName", "mcpCounter"));
         callTool("increment", Json.object().set("counterName", "mcpCounter"));

         String value = callToolText("getCounter", Json.object()
               .set("counterName", "mcpCounter"));
         assertTrue(value.contains("2"));

         callTool("decrement", Json.object().set("counterName", "mcpCounter"));

         value = callToolText("getCounter", Json.object()
               .set("counterName", "mcpCounter"));
         assertTrue(value.contains("1"));
      } finally {
         // counters cannot be removed via MCP, but they will be cleaned up with the cache manager
      }
   }

   @Test
   public void testClusterInfo() {
      String info = callToolText("getClusterInfo", Json.object());
      assertNotNull(info);
      Json cluster = Json.read(info);
      assertEquals("RUNNING", cluster.at("cache_manager_status").asString());
   }

   @Test
   public void testJvmInfo() {
      String info = callToolText("getJvmInfo", Json.object());
      assertNotNull(info);
      Json jvm = Json.read(info);
      assertNotNull(jvm.at("jvm_name").asString());
      assertTrue(jvm.at("uptime_seconds").asInteger() >= 0);
   }

   @Test
   public void testJvmMemory() {
      String info = callToolText("getJvmMemory", Json.object());
      assertNotNull(info);
      Json memory = Json.read(info);
      assertNotNull(memory.at("heap"));
      assertTrue(memory.at("heap").at("max_mb").asInteger() > 0);
   }

   @Test
   public void testJvmThreads() {
      String info = callToolText("getJvmThreads", Json.object());
      assertNotNull(info);
      Json threads = Json.read(info);
      assertTrue(threads.at("thread_count").asInteger() > 0);
      assertEquals(0, threads.at("deadlocked_threads").asInteger());
   }

   @Test
   public void testGetServerConfiguration() {
      String config = callToolText("getServerConfiguration", Json.object());
      assertNotNull(config);
      Json parsed = Json.read(config);
      assertNotNull(parsed.at("infinispan"));
   }

   @Test
   public void testToolNotFound() {
      Json response = mcpCall(toolCall("nonExistentTool", Json.object()));
      assertNotNull(response.at("error"));
      assertEquals(-32601, response.at("error").at("code").asInteger());
   }
}
