package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.util.List;

import org.infinispan.server.resp.json.JSONUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.json.DefaultJsonParser;
import io.lettuce.core.json.JsonArray;
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
   private Configuration configList = JSONUtil.configList;

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
      assertThat(jv.asJsonArray().size()).isEqualTo(1);
      jv = jv.asJsonArray().get(0);
      JsonValue jv1 = new DefaultJsonParser().createJsonValue("{\"key2\":\"value2\"}");
      assertThat(redis.jsonSet(key, jp, jv1)).isEqualTo("OK");
      result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jv, "$.key.key2", jv1)).isEqualTo(true);
   }

   @Test
   public void testJSONSETLegacy() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      String key = k();
      String v = "{\"key\":\"value\"}";
      var jv = new DefaultJsonParser().createJsonValue(v);
      String p = ".";
      var jp = new JsonPath(p);
      assertThat(command.jsonSet(key, ".", v)).isEqualTo("OK");
      var result = new DefaultJsonParser().createJsonValue(command.jsonGet(key, "."));
      // No need to wrap since jv is a legacy path
      assertThat(compareJSONGet(result, jv, jp)).isEqualTo(true);

      // Test root can be updated
      v = """
            {
            "key": { "key1": "val1" }
            }
            """;
      jv = new DefaultJsonParser().createJsonValue(v);
      assertThat(command.jsonSet(key, ".", v)).isEqualTo("OK");
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(key, ""));
      result = wrapInArray(result);
      assertThat(compareJSON(result, jv)).isEqualTo(true);

      // Test adding a field in a leaf
      String v1 = "{\"key2\":\"value2\"}";
      var jv1 = new DefaultJsonParser().createJsonValue(v1);
      assertThat(command.jsonSet(key, ".key.key", v1)).isEqualTo("OK");
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(key, ""));
      result = wrapInArray(result);
      assertThat(compareJSONSet(result, jv, ".key.key", jv1)).isEqualTo(true);
   }

   @Test
   public void testJSONSETWrongType() {
      String key = k();
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      JsonPath jp = new JsonPath("$");

      // Check get a string as json
      assertWrongType(() -> redis.set(key, v()), () -> redis.jsonGet(key, new JsonPath("$")));
      // Check set an existing string
      assertWrongType(() -> {
      }, () -> redis.jsonSet(key, jp, jv));
      // Check with non root
      String k1 = k(1);
      assertWrongType(() -> redis.set(k1, v()), () -> redis.jsonGet(k1, new JsonPath("$.k1")));
      // Check json is not a string
      String k2 = k(2);
      assertWrongType(() -> redis.jsonSet(k2, jp, jv), () -> redis.get(k2));
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
   public void testJSONSETInvalidPath() {
      String key = k();
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = new DefaultJsonParser().createJsonValue("{\"root\": { \"k1\" : \"v1\", \"k2\":\"v2\"}}");
      assertThat(redis.jsonSet(key, jpRoot, jvDoc)).isEqualTo("OK");

      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      // Modify json
      assertThatThrownBy(() -> {
         command.jsonSet(key, "", "\"newValue\"");
      }).isInstanceOf(RedisCommandExecutionException.class);
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
      var result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jvDoc, "$..k1", jvNew)).isEqualTo(true);
   }

   @Test
   public void testJSONSETWithXX() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"k\":\"v\"}");
      JsonValue jvNew = new DefaultJsonParser().createJsonValue("{\"kNew\":\"vNew\"}");
      JsonSetArgs args = JsonSetArgs.Builder.xx();
      assertThat(redis.jsonSet(key, jp, jv, args)).isNull();
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      assertThat(redis.jsonSet(key, jp, jvNew, args)).isEqualTo("OK");
      var result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jvNew)).isEqualTo(true);
      // Test non root behaviour
      JsonPath jp1 = new JsonPath("$.key1");
      JsonValue jv1New = new DefaultJsonParser().createJsonValue("{\"k1\":\"v1\"}");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isNull();
      assertThat(redis.jsonSet(key, jp1, jv)).isEqualTo("OK");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isEqualTo("OK");
   }

   @Test
   public void testJSONSETWithNX() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"k\":\"v\"}");
      JsonSetArgs args = JsonSetArgs.Builder.nx();
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jv)).isEqualTo(true);
      assertThat(redis.jsonSet(key, jp, jv, args)).isNull();
      // Test non root behaviour
      JsonPath jp1 = new JsonPath("$.key1");
      JsonValue jv1New = new DefaultJsonParser().createJsonValue("{\"k1\":\"v1\"}");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isEqualTo("OK");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isNull();
   }

   @Test
   public void testJSONSETNotRoot() {
      JsonPath jp = new JsonPath("$.notroot");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"k1\":\"v1\"}");
      assertThatThrownBy(() -> {
         redis.jsonSet(k(), jp, jv);
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
      List<JsonValue> result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), doc, ".key1", newNode)).isEqualTo(true);

      doc = result.get(0);
      jp = new JsonPath("$.key");
      newNode = new DefaultJsonParser().createJsonValue("{\"key_l1\":\"value_l1\"}");
      assertThat(redis.jsonSet(key, jp, newNode)).isEqualTo("OK");
      result = redis.jsonGet(key, new JsonPath("$"));
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
   public void testJSONGETLegacy() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("""
               { "key1":"value1",
                 "key2":"value2"
               }
            """);
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      String p1 = ".key1";
      JsonPath jp1 = new JsonPath(p1);
      var result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p1));
      // No need to wrap
      assertThat(compareJSONGet(result, jv, jp1)).isEqualTo(true);

      String p2 = ".key2";
      JsonPath jp2 = new JsonPath(p2);
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p1, p2));
      // With legacy no need to wrap the root object in array
      assertThat(compareJSONGet(result, jv, jp1, jp2)).isEqualTo(true);

      String p3 = ".key3";
      JsonPath jp3 = new JsonPath(p3);
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p1, p2, p3));
      assertThat(compareJSONGet(result, jv, jp1, jp2, jp3)).isEqualTo(true);

      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p3));
      assertThat(compareJSONGet(result, jv, jp3)).isEqualTo(true);

      // One path multiple results
      String p4 = ".*";
      JsonPath jp4 = new JsonPath(p4);
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p4));
      assertThat(compareJSONGet(result, jv, jp4)).isEqualTo(true);

      // Multiple path multiple results
      String p5 = ".key1";
      JsonPath jp5 = new JsonPath(p5);
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p4, p5));
      assertThat(compareJSONGet(result, jv, jp4, jp5)).isEqualTo(true);

      // Mixing legacy and not legacy path
      String p6 = "$.key3";
      JsonPath jp6 = new JsonPath(p6);
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p1, p2, p6));
      assertThat(compareJSONGet(result, jv, jp1, jp2, jp6)).isEqualTo(true);

      // Multiple path multiple results, legacy and not
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p4, p6));
      assertThat(compareJSONGet(result, jv, jp4, jp6)).isEqualTo(true);
   }

   @Test
   public void testJSONGETLegacyError() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      assertThatThrownBy(() -> {
         command.jsonSet(k(), "..", "{ \"k1\": \"v1\"}");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");
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
            [21{211"key1":3"value1",211"key2":3"value2"21}2]""";
      assertThat(strResult).isEqualTo(expected);
   }

   @Test
   public void testJSONSETWhiteSpaces() {
      String value = """
            { \t"k1"\u000d:\u000a"v1"}
             """;
      JsonValue jv = new DefaultJsonParser().createJsonValue(value);
      JsonPath jp = new JsonPath("$");
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      List<JsonValue> result = redis.jsonGet(k(), new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jv)).isTrue();
   }

   private boolean compareJSONGet(JsonValue result, JsonValue expected, JsonPath... paths) {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootObjectNode, resultNode;
      if (paths == null) {
         paths = new JsonPath[] { new JsonPath("$") };
      }
      try {
         rootObjectNode = (JsonNode) mapper.readTree(expected.toString());
         resultNode = (JsonNode) mapper.readTree(result.toString());
         var jpCtx = com.jayway.jsonpath.JsonPath.using(configList).parse(rootObjectNode);
         boolean isLegacy = true;
         // If all paths are legacy, return results in legacy mode. i.e. no array
         for (JsonPath path : paths) {
            isLegacy &= !JSONUtil.isJsonPath(path.toString());
         }
         if (paths.length == 1) {
            // jpctx.read doesn't like legacy ".", change it to "$". everything else seems to work
            String pathStr = ".".equals(paths[0].toString()) ? "$" : paths[0].toString();
            JsonNode node = isLegacy ? ((ArrayNode) jpCtx.read(pathStr)).get(0)
                  : jpCtx.read(pathStr);
            return resultNode.equals(node);
         }
         ObjectNode root = mapper.createObjectNode();
         for (JsonPath path : paths) {
            String pathStr = path.toString();
            JsonNode node = isLegacy ? ((ArrayNode) jpCtx.read(pathStr)).get(0)
                  : jpCtx.read(pathStr);
            root.set(pathStr, node);
         }
         return resultNode.equals(root);
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      }
   }

   private boolean compareJSON(JsonValue result, JsonValue expected) {
      return compareJSONGet(result, expected, (JsonPath[]) null);
   }

   private boolean compareJSONSet(JsonValue newDoc, JsonValue oldDoc, String path, JsonValue node) {
      ObjectMapper mapper = new ObjectMapper();
      try {
         var newRootNode = mapper.readTree(unwrapIfArray(newDoc).toString());
         var oldRootNode = mapper.readTree(unwrapIfArray(oldDoc).toString());
         // Unwrap objects if in an array
         var jpCtx = com.jayway.jsonpath.JsonPath.using(configList).parse(oldRootNode);
         var pathStr = new String(path);
         var newNode = mapper.readTree(node.toString());
         jpCtx.set(pathStr, newNode);
         // Check the whole doc is correct
         if (!oldRootNode.equals(newRootNode)) {
            return false;
         }
         // Check the node is set correctly
         var newJpCtx = com.jayway.jsonpath.JsonPath.using(configList).parse(newRootNode);
         var newNodeFromNewDoc = newJpCtx.read(pathStr);
         var expectedNode = jpCtx.read(pathStr);
         return newNodeFromNewDoc.equals(expectedNode);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private JsonValue wrapInArray(JsonValue v) {
      JsonArray arr = new DefaultJsonParser().createJsonArray();
      return arr.add(v);
   }

   private JsonValue unwrapIfArray(JsonValue v) {
      if (v.isJsonArray()) {
         var arr = v.asJsonArray();
         if (arr.size() == 0)
            return null;
         if (arr.size() == 1)
            return arr.get(0);
         throw new RuntimeException("Argument has size > 1");
      }
      return v;
   }
}
