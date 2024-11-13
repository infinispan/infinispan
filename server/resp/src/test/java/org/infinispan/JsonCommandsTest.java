package org.infinispan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.test.TestingUtil.k;
import static org.junit.jupiter.api.Assertions.fail;

import org.infinispan.server.resp.CustomStringCommands;
import org.infinispan.server.resp.SingleNodeRespBaseTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.json.DefaultJsonParser;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.json.JsonValue;
import io.lettuce.core.json.arguments.JsonSetArgs;

@Test(groups = "functional", testName = "server.resp.JsonCommandsTest")
public class JsonCommandsTest extends SingleNodeRespBaseTest {
   /**
    * RESP Sorted set commands testing
    *
    * @since 15.1
    */
   RedisCommands<String, String> redis;
   private Configuration config = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider())
         .build();

   @BeforeMethod
   public void initConnection() {
      redis = redisConnection.sync();
   }

   @Test
   public void testJSONSET() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(k(), jp);
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jv)).isEqualTo(true);
   }

   @Test
   public void testJSONSETWithPath() {
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = new DefaultJsonParser().createJsonValue("{\"root\": { \"k1\" : \"v1\", \"k2\":\"v2\"}}");
      assertThat(redis.jsonSet(k(), jpRoot, jvDoc)).isEqualTo("OK");

      // Modify json
      JsonPath jpLeaf = new JsonPath("$.root.k1");
      JsonValue jvNew = new DefaultJsonParser().createJsonValue("\"newv1\"");
      assertThat(redis.jsonSet(k(), jpLeaf, jvNew)).isEqualTo("OK");

      // Verify
      var result = redis.jsonGet(k(), jpRoot);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jvDoc, "$.root.k1", jvNew));
   }

   @Test
   public void testJSONSETWithPathMulti() {
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = new DefaultJsonParser().createJsonValue(
            """
                  {"r1": { "k1" : "v1", "k2":"v2"}, "r2": { "k1" : "v1", "k2": "v2"}}"
                  """);
      assertThat(redis.jsonSet(k(), jpRoot, jvDoc)).isEqualTo("OK");

      // Modify json
      JsonPath jpLeaf = new JsonPath("$..k1");
      JsonValue jvNew = new DefaultJsonParser().createJsonValue("\"newv1\"");
      assertThat(redis.jsonSet(k(), jpLeaf, jvNew)).isEqualTo("OK");

      // Verify
      var result = redis.jsonGet(k(), jpRoot);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jvDoc, "$..k1", jvNew)).isEqualTo(true);
   }

   @Test
   public void testJSONSETWithXX() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      JsonSetArgs args = JsonSetArgs.Builder.xx();
      assertThat(redis.jsonSet(k(), jp, jv, args)).isNull();
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
   }

   @Test
   public void testJSONSETNotRoot() {
      CustomStringCommands commands = CustomStringCommands.instance(redisConnection);
      assertThatThrownBy(() -> {
         commands.jsonCmd(k(), "notroot", "{ \"k1\": \"v1\"}");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR new objects must be created at root");
   }

   @Test
   public void testJSONSETWrongPath() {
      CustomStringCommands commands = CustomStringCommands.instance(redisConnection);
      commands.jsonCmd(k(), "$", "{ \"k1\": \"v1\"}");
      assertThatThrownBy(() -> {
         commands.jsonCmd(k(), "b a d", "{ \"k1\": \"v1\"}");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");
   }

   @Test
   public void testJSONSETPaths() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");

      jp = new JsonPath("$.key1");
      var jvNew = new DefaultJsonParser().createJsonValue("\"value1\"");
      assertThat(redis.jsonSet(k(), jp, jvNew)).isEqualTo("OK");
      var result = redis.jsonGet(k(), jp);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jv, "$.key1", jvNew)).isEqualTo(true);

      jv = result.get(0);
      jp = new JsonPath("$.key");
      jvNew = new DefaultJsonParser().createJsonValue("{\"key_l1\":\"value_l1\"}");
      assertThat(redis.jsonSet(k(), jp, jvNew)).isEqualTo("OK");
      result = redis.jsonGet(k(), jp);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jv, "$.key", jvNew)).isEqualTo(true);

      jp = new JsonPath("$.lev1");
      jv = new DefaultJsonParser().createJsonValue("{\"keyl1\":\"valuel1\"}");
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");

      jp = new JsonPath("$.lev1.lev2.lev3");
      jv = new DefaultJsonParser().createJsonValue("{\"keyl2\":\"valuel2\"}");
      assertThat(redis.jsonSet(k(), jp, jv)).isNull();
   }

   @Test
   public void testJSONGET() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("""
               { "key1":"value1",
                 "key2":"value2"
               }
            """);
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      JsonPath jp1 = new JsonPath("$.key1");
      JsonPath jp2 = new JsonPath("$.key2");
      JsonPath jp3 = new JsonPath("$.key3");
      var result = redis.jsonGet(k(), jp1);
      assertThat(compareJSONGet(result.get(0), jv, jp1)).isEqualTo(true);

      result = redis.jsonGet(k(), jp1, jp2);
      assertThat(compareJSONGet(result.get(0), jv, jp1, jp2)).isEqualTo(true);

      result = redis.jsonGet(k(), jp1, jp2, jp3);
      assertThat(compareJSONGet(result.get(0), jv, jp1, jp2, jp3)).isEqualTo(true);

      result = redis.jsonGet(k(), jp3);
      assertThat(compareJSONGet(result.get(0), jv, jp3)).isEqualTo(true);
   }

   private boolean compareJSONGet(JsonValue result, JsonValue doc, JsonPath... paths) {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode rootObjectNode, resultNode;
      try {
         rootObjectNode = (ObjectNode) mapper.readTree(doc.toString());
         resultNode = (ObjectNode) mapper.readTree(result.toString());
         var jpCtx = com.jayway.jsonpath.JsonPath.using(config).parse(rootObjectNode);
         if (paths.length == 1) {
            JsonNode node = jpCtx.read(paths[0].toString());
            return resultNode.equals(node);
         }
         int pos = 0;
         ObjectNode root = mapper.createObjectNode();
         while (pos < paths.length) {
            JsonNode node = jpCtx.read(paths[pos].toString());
            root.set(paths[pos++].toString(), node);
         }
         return resultNode.equals(root);
      } catch (Exception ex) {
         fail();
         return false;
      }
   }

   private boolean compareJSON(JsonValue j1, JsonValue j2) {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode jsonNode;
      try {
         jsonNode = mapper.readTree(j1.toString());
         var jsonNode2 = mapper.readTree(j2.toString());
         return jsonNode.equals(jsonNode2);
      } catch (Exception e) {
         fail();
      }
      return false;
   }

   private boolean compareJSONSet(JsonValue result, JsonValue doc, String path, JsonValue node) {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode rootObjectNode;
      try {
         rootObjectNode = (ObjectNode) mapper.readTree(doc.toString());
         var jpCtx = com.jayway.jsonpath.JsonPath.using(config).parse(rootObjectNode);
         var pathStr = new String(path);
         var leaf = jpCtx.read(pathStr);
         JsonNode newNode = mapper.readTree(node.toString());
         if (leaf == null) {
            jpCtx.put(pathStr, "", newNode);
         } else {
            jpCtx.set(pathStr, newNode);
         }
      } catch (Exception e) {
         fail();
         return false;
      }
      try {
         var jsonRes = mapper.readTree(result.toString());
         return jsonRes.equals(rootObjectNode);
      } catch (Exception e) {
         fail();
      }
      return false;
   }
}
