package org.infinispan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
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
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

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

      // Test root can be updated
      jp = new JsonPath("$");
      jv = new DefaultJsonParser().createJsonValue("""
            {
            "key": { "key1": "val1" }
            }
            """);
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      result = redis.jsonGet(k(), jp);
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jv)).isEqualTo(true);

      // Test adding a field in a leaf
      jp = new JsonPath("$.key.key2");
      jv = result.get(0);
      JsonValue jv1 = new DefaultJsonParser().createJsonValue("{\"key2\":\"value2\"}");
      assertThat(redis.jsonSet(k(), jp, jv1)).isEqualTo("OK");
      result = redis.jsonGet(k(), jp);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(jv, "$.key.key2", jv1, result.get(0))).isEqualTo(true);

   }

   @Test
   public void testJSONSETWrongType() {
      assertWrongType(() -> redis.set(k(), v()), () -> redis.jsonGet(k(), new JsonPath("$")));
      // Check with non root
      String k1 = k(1);
      assertWrongType(() -> redis.set(k1, v()), () -> redis.jsonGet(k1, new JsonPath("$.k1")));
      // Check json is not a string
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      String k2 = k(2);
      assertWrongType(() -> redis.jsonSet(k2, new JsonPath("$"), jv), () -> redis.get(k2));
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
      assertThat(compareJSONSet(jvDoc, "$.root.k1", jvNew, result.get(0)));
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
      assertThat(compareJSONSet(jvDoc, "$..k1", jvNew, result.get(0))).isEqualTo(true);
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
      assertThat(compareJSONSet(jv, "$.key1", jvNew, result.get(0))).isEqualTo(true);

      jv = result.get(0);
      jp = new JsonPath("$.key");
      jvNew = new DefaultJsonParser().createJsonValue("{\"key_l1\":\"value_l1\"}");
      assertThat(redis.jsonSet(k(), jp, jvNew)).isEqualTo("OK");
      result = redis.jsonGet(k(), jp);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(jv, "$.key", jvNew, result.get(0))).isEqualTo(true);

      jp = new JsonPath("$.lev1");
      jv = new DefaultJsonParser().createJsonValue("{\"keyl1\":\"valuel1\"}");
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");

      jp = new JsonPath("$.lev1.lev2.lev3");
      jv = new DefaultJsonParser().createJsonValue("{\"keyl2\":\"valuel2\"}");
      assertThat(redis.jsonSet(k(), jp, jv)).isNull();
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

   private boolean compareJSONSet(JsonValue doc, String path, JsonValue node, JsonValue result) {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode rootObjectNode;
      try {
         rootObjectNode = (ObjectNode) mapper.readTree(doc.toString());
         var jpCtx = com.jayway.jsonpath.JsonPath.using(config).parse(rootObjectNode);
         var pathStr = new String(path);
         JsonNode newNode = mapper.readTree(node.toString());
         jpCtx.set(pathStr, newNode);
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
