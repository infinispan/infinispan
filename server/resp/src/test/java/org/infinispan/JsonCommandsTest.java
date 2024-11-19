package org.infinispan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;

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
import io.lettuce.core.json.arguments.JsonGetArgs;
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
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key, jp);
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jv)).isEqualTo(true);

      // Test root can be updated
      jp = new JsonPath("$");
      jv = new DefaultJsonParser().createJsonValue("""
            {
            "key": { "key1": "val1" }
            }
            """);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      result = redis.jsonGet(key, jp);
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jv)).isEqualTo(true);

      // Test adding a field in a leaf
      jp = new JsonPath("$.key.key2");
      jv = result.get(0);
      JsonValue jv1 = new DefaultJsonParser().createJsonValue("{\"key2\":\"value2\"}");
      assertThat(redis.jsonSet(key, jp, jv1)).isEqualTo("OK");
      result = redis.jsonGet(key);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jv, "$.key.key2", jv1)).isEqualTo(true);

   }

   @Test
   public void testJSONSETWrongType() {
      String key = k();
      assertWrongType(() -> redis.set(key, v()), () -> redis.jsonGet(key, new JsonPath("$")));
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
      String key = k();
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = new DefaultJsonParser().createJsonValue("{\"root\": { \"k1\" : \"v1\", \"k2\":\"v2\"}}");
      assertThat(redis.jsonSet(key, jpRoot, jvDoc)).isEqualTo("OK");

      // Modify json
      JsonPath jpLeaf = new JsonPath("$.root.k1");
      JsonValue jvNew = new DefaultJsonParser().createJsonValue("\"newv1\"");
      assertThat(redis.jsonSet(key, jpLeaf, jvNew)).isEqualTo("OK");

      // Verify
      var result = redis.jsonGet(key, jpRoot);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jvDoc, "$.root.k1", jvNew));
   }

   @Test
   public void testJSONSETWithPathMulti() {
      String key = k();
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = new DefaultJsonParser().createJsonValue(
            """
                  {"r1": { "k1" : "v1", "k2":"v2"}, "r2": { "k1" : "v1", "k2": "v2"}}"
                  """);
      assertThat(redis.jsonSet(key, jpRoot, jvDoc)).isEqualTo("OK");

      // Modify json
      JsonPath jpLeaf = new JsonPath("$..k1");
      JsonValue jvNew = new DefaultJsonParser().createJsonValue("\"newv1\"");
      assertThat(redis.jsonSet(key, jpLeaf, jvNew)).isEqualTo("OK");

      // Verify
      var result = redis.jsonGet(key);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jvDoc, "$..k1", jvNew)).isEqualTo(true);
   }

   @Test
   public void testJSONSETWithXX() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      JsonSetArgs args = JsonSetArgs.Builder.xx();
      assertThat(redis.jsonSet(key, jp, jv, args)).isNull();
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
   }

   @Test
   public void testJSONSETNotRoot() {
      CustomStringCommands commands = CustomStringCommands.instance(redisConnection);
      assertThatThrownBy(() -> {
         commands.jsonSet(k(), "notroot", "{ \"k1\": \"v1\"}");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR new objects must be created at root");
   }

   @Test
   public void testJSONSETWrongPath() {
      CustomStringCommands commands = CustomStringCommands.instance(redisConnection);
      commands.jsonSet(k(), "$", "{ \"k1\": \"v1\"}");
      assertThatThrownBy(() -> {
         commands.jsonSet(k(), "b a d", "{ \"k1\": \"v1\"}");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");
   }

   @Test
   public void testJSONSETPaths() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue doc = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      assertThat(redis.jsonSet(key, jp, doc)).isEqualTo("OK");

      jp = new JsonPath("$.key1");
      var newNode = new DefaultJsonParser().createJsonValue("\"value1\"");
      assertThat(redis.jsonSet(key, jp, newNode)).isEqualTo("OK");
      List<JsonValue> result = redis.jsonGet(key);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), doc, "$.key1", newNode)).isEqualTo(true);

      doc = result.get(0);
      jp = new JsonPath("$.key");
      newNode = new DefaultJsonParser().createJsonValue("{\"key_l1\":\"value_l1\"}");
      assertThat(redis.jsonSet(key, jp, newNode)).isEqualTo("OK");
      result = redis.jsonGet(key);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), doc, "$.key", newNode)).isEqualTo(true);

      jp = new JsonPath("$.lev1");
      doc = new DefaultJsonParser().createJsonValue("{\"keyl1\":\"valuel1\"}");
      assertThat(redis.jsonSet(key, jp, doc)).isEqualTo("OK");

      jp = new JsonPath("$.lev1.lev2.lev3");
      doc = new DefaultJsonParser().createJsonValue("{\"keyl2\":\"valuel2\"}");
      assertThat(redis.jsonSet(key, jp, doc)).isNull();
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
      var result = redis.jsonGet(k(), jp1);
      assertThat(compareJSONGet(result.get(0), jv, jp1)).isEqualTo(true);

      JsonPath jp2 = new JsonPath("$.key2");
      result = redis.jsonGet(k(), jp1, jp2);
      assertThat(compareJSONGet(result.get(0), jv, jp1, jp2)).isEqualTo(true);

      JsonPath jp3 = new JsonPath("$.key3");
      result = redis.jsonGet(k(), jp1, jp2, jp3);
      assertThat(compareJSONGet(result.get(0), jv, jp1, jp2, jp3)).isEqualTo(true);

      result = redis.jsonGet(k(), jp3);
      assertThat(compareJSONGet(result.get(0), jv, jp3)).isEqualTo(true);

      // One path multiple results
      JsonPath jp4 = new JsonPath("$.*");
      result = redis.jsonGet(k(), jp4);
      assertThat(compareJSONGet(result.get(0), jv, jp4)).isEqualTo(true);

      // Multiple path multiple results
      JsonPath jp5 = new JsonPath("$.key1");
      result = redis.jsonGet(k(), jp4, jp5);
      assertThat(compareJSONGet(result.get(0), jv, jp4, jp5)).isEqualTo(true);
   }

   @Test
   public void testJSONGETPrettyPrinter() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("""
               { "key1":"value1",
                 "key2":"value2"
               }
            """);
      String key = k();
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      JsonPath jp1 = new JsonPath("$");
      JsonGetArgs args = new JsonGetArgs().indent("1").newline("2").space("3");
      var result = redis.jsonGet(key, args, jp1);
      assertThat(result).hasSize(1);
      String strResult = result.get(0).toString();
      String expected = """
      {21"key1":3"value1",21"key2":3"value2"2}""";
      assertThat(strResult).isEqualTo(expected);
   }

   private boolean compareJSONGet(JsonValue result, JsonValue doc, JsonPath... paths) {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootObjectNode, resultNode;
      try {
         rootObjectNode = (JsonNode) mapper.readTree(doc.toString());
         resultNode = (JsonNode) mapper.readTree(result.toString());
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

   private boolean compareJSONSet(JsonValue newDoc, JsonValue oldDoc, String path, JsonValue node) {
      ObjectMapper mapper = new ObjectMapper();
      try {
         var newRootNode = mapper.readTree(newDoc.toString());
         var oldRootNode = (ObjectNode) mapper.readTree(oldDoc.toString());
         var jpCtx = com.jayway.jsonpath.JsonPath.using(config).parse(oldRootNode);
         var pathStr = new String(path);
         var newNode = mapper.readTree(node.toString());
         jpCtx.set(pathStr, newNode);
         // Check the whole doc is correct
         if (!oldRootNode.equals(newRootNode)) {
            return false;
         }
         // Check the node is set correctly
         var newJpCtx = com.jayway.jsonpath.JsonPath.using(config).parse(newRootNode);
         var newNodeFromNewDoc = newJpCtx.read(pathStr);
         var expectedNode = jpCtx.read(pathStr);
         return newNodeFromNewDoc.equals(expectedNode);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
