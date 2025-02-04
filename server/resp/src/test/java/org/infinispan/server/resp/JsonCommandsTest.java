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
    * RESP JSON commands testing
    *
    * @since 15.2
    */
   private RedisCommands<String, String> redis;
   private DefaultJsonParser defaultJsonParser = new DefaultJsonParser();

   @BeforeMethod
   public void initConnection() {
      redis = redisConnection.sync();
   }

   @Test
   public void testJSONSET() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("{\"key\":\"value\"}");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key, jp);
      assertThat(compareJSONGet(result, jv)).isEqualTo(true);

      // Test root can be updated
      jv = defaultJsonParser.createJsonValue("""
            {
            "key1a": { "key2a": "val2a" }
            }
            """);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      result = redis.jsonGet(key, jp);
      assertThat(compareJSONGet(result, jv)).isEqualTo(true);

      // Test adding a field in a leaf
      jp = new JsonPath("$.key1a.key2b");
      jv = result.get(0);
      assertThat(jv.asJsonArray().size()).isEqualTo(1);
      jv = jv.asJsonArray().getFirst();
      JsonValue jv1 = defaultJsonParser.createJsonValue("{\"key2a\":\"newVal2\"}");
      assertThat(redis.jsonSet(key, jp, jv1)).isEqualTo("OK");
      result = redis.jsonGet(key);
      assertThat(compareJSONSet(result, jv, "$.key1a.key2b", jv1)).isEqualTo(true);
   }

   @Test
   public void testJSONSETAddNode() {
      String key = k();
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("{\"key1a\":\"val1a\"}");
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");
      // Add leaf to string should fail
      JsonPath jp = new JsonPath("$.key1a.key2b");
      jv = defaultJsonParser.createJsonValue("\"val2b\"");
      assertThat(redis.jsonSet(key, jp, jv)).isNull();
      jp = new JsonPath("$.key1b");
      jv = defaultJsonParser.createJsonValue("\"val1b\"");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      // Add leaf to root object should succeded
      JsonValue jv0 = redis.jsonGet(key, jpRoot).get(0);
      jv = defaultJsonParser.createJsonValue("{\"key2a\":\"val2a\"}");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key, jpRoot);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0).asJsonArray().getFirst(), jv0.asJsonArray().getFirst(), "$.key1b", jv))
            .isEqualTo(true);
      // Add leaf to leaf object should succeded
      jv0 = redis.jsonGet(key, jpRoot).get(0);
      jv = defaultJsonParser.createJsonValue("\"val2b\"");
      jp = new JsonPath("$.key1b.key2b");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      result = redis.jsonGet(key, jpRoot);
      assertThat(result).hasSize(1);
      assertThat(
            compareJSONSet(result.get(0).asJsonArray().getFirst(), jv0.asJsonArray().getFirst(), "$.key1b.key2b", jv))
            .isEqualTo(true);
   }

   @Test
   public void testJSONSETLegacy() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      String key = k();
      String doc = "{\"key\":\"value\"}";
      var json = defaultJsonParser.createJsonValue(doc);
      String p = ".";
      var jp = new JsonPath(p);
      // Test create root object
      assertThat(command.jsonSet(key, ".", doc)).isEqualTo("OK");
      var result = defaultJsonParser.createJsonValue(command.jsonGet(key, "."));
      assertThat(compareJSONGet(result, json, jp)).isEqualTo(true);
      // Test root can be updated
      doc = """
            {
            "key": { "key1": "val1" }
            }
            """;
      assertThat(command.jsonSet(key, ".", doc)).isEqualTo("OK");
      result = defaultJsonParser.createJsonValue(command.jsonGet(key, "."));
      json = defaultJsonParser.createJsonValue(doc);
      assertThat(compareJSONGet(result, json, new JsonPath("."))).isEqualTo(true);
      // Test adding a field in a leaf
      String v1 = "{\"key2\":\"value2\"}";
      assertThat(command.jsonSet(key, ".key.key", v1)).isEqualTo("OK");
      result = defaultJsonParser.createJsonValue(command.jsonGet(key, "."));
      var jv1 = defaultJsonParser.createJsonValue(v1);
      assertThat(compareJSONSet(result, json, "$.key.key", jv1)).isEqualTo(true);
   }

   @Test
   public void testJSONSETWithPath() {
      String key = k();
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = defaultJsonParser.createJsonValue("{\"root\": { \"k1\" : \"v1\", \"k2\":\"v2\"}}");
      assertThat(redis.jsonSet(key, jpRoot, jvDoc)).isEqualTo("OK");
      // Modify json
      JsonPath jpLeaf = new JsonPath("$.root.k1");
      JsonValue jvNew = defaultJsonParser.createJsonValue("\"newv1\"");
      assertThat(redis.jsonSet(key, jpLeaf, jvNew)).isEqualTo("OK");
      // Verify
      var result = redis.jsonGet(key, jpRoot);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0).asJsonArray().getFirst(), jvDoc, "$.root.k1", jvNew));
   }

   @Test
   public void testJSONSETInvalidPath() {
      String key = k();
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = defaultJsonParser.createJsonValue("{\"root\": { \"k1\" : \"v1\", \"k2\":\"v2\"}}");
      assertThat(redis.jsonSet(key, jpRoot, jvDoc)).isEqualTo("OK");
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      // Modify json
      assertThatThrownBy(() -> {
         command.jsonSet(key, "", "\"newValue\"");
      }).isInstanceOf(RedisCommandExecutionException.class);
      assertThatThrownBy(() -> {
         command.jsonSet(key, "$", "{not-a-json");
      }).isInstanceOf(RedisCommandExecutionException.class);
   }

   @Test
   public void testJSONSETWithPathMulti() {
      String key = k();
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = defaultJsonParser.createJsonValue(
            """
                  {"r1": { "k1" : "v1", "k2":"v2"}, "r2": { "k1" : "v1", "k2": "v2"}, "r3": { "k2": "v2"}}
                  """);
      assertThat(redis.jsonSet(key, jpRoot, jvDoc)).isEqualTo("OK");
      // Modify json
      JsonPath jpLeaf = new JsonPath("$..k1");
      JsonValue jvNew = defaultJsonParser.createJsonValue("\"newv1\"");
      assertThat(redis.jsonSet(key, jpLeaf, jvNew)).isEqualTo("OK");
      // Verify
      var result = redis.jsonGet(key);
      assertThat(compareJSONSet(result, jvDoc, "$..k1", jvNew)).isEqualTo(true);
   }

   @Test
   public void testJSONSETWithXX() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("{\"k\":\"v\"}");
      JsonValue jvNew = defaultJsonParser.createJsonValue("{\"kNew\":\"vNew\"}");
      JsonSetArgs args = JsonSetArgs.Builder.xx();
      assertThat(redis.jsonSet(key, jp, jv, args)).isNull();
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      assertThat(redis.jsonSet(key, jp, jvNew, args)).isEqualTo("OK");
      var result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(compareJSONGet(result, jvNew)).isEqualTo(true);
      // Test non root behaviour
      JsonPath jp1 = new JsonPath("$.key1");
      JsonValue jv1New = defaultJsonParser.createJsonValue("{\"k1\":\"v1\"}");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isNull();
      assertThat(redis.jsonSet(key, jp1, jv)).isEqualTo("OK");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isEqualTo("OK");
   }

   @Test
   public void testJSONSETWithNX() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("{\"k\":\"v\"}");
      JsonSetArgs args = JsonSetArgs.Builder.nx();
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(compareJSONGet(result, jv)).isEqualTo(true);
      assertThat(redis.jsonSet(key, jp, jv, args)).isNull();
      // Test non root behaviour
      JsonPath jp1 = new JsonPath("$.key1");
      JsonValue jv1New = defaultJsonParser.createJsonValue("{\"k1\":\"v1\"}");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isEqualTo("OK");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isNull();
   }

   @Test
   public void testJSONSETNotRoot() {
      JsonPath jp = new JsonPath("$.notroot");
      JsonValue jv = defaultJsonParser.createJsonValue("{\"k1\":\"v1\"}");
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
      JsonValue doc = defaultJsonParser.createJsonValue("{\"key\":\"value\"}");
      assertThat(redis.jsonSet(key, jp, doc)).isEqualTo("OK");
      // Setting string value1 for $.key1
      jp = new JsonPath("$.key1");
      var newNode = defaultJsonParser.createJsonValue("\"value1\"");
      assertThat(redis.jsonSet(key, jp, newNode)).isEqualTo("OK");
      List<JsonValue> result = redis.jsonGet(key);
      assertThat(compareJSONSet(result, doc, "$.key1", newNode)).isEqualTo(true);

      doc = result.get(0);
      jp = new JsonPath("$.key");
      newNode = defaultJsonParser.createJsonValue("{\"key_l1\":\"value_l1\"}");
      assertThat(redis.jsonSet(key, jp, newNode)).isEqualTo("OK");
      result = redis.jsonGet(key);
      assertThat(compareJSONSet(result, doc, "$.key", newNode)).isEqualTo(true);

      jp = new JsonPath("$.lev1");
      doc = defaultJsonParser.createJsonValue("{\"keyl1\":\"valuel1\"}");
      assertThat(redis.jsonSet(key, jp, doc)).isEqualTo("OK");

      jp = new JsonPath("$.lev1.lev2.lev3");
      doc = defaultJsonParser.createJsonValue("{\"keyl2\":\"valuel2\"}");
      assertThat(redis.jsonSet(key, jp, doc)).isNull();
   }

   @Test
   public void testJSONSETGetMultiPath() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonPath jpMulti = new JsonPath("$..b");
      String value = """
            {"a1":{"b":{"c":true,"d":[], "e": [1,2,3,4]}},"a2":{"b":{"c":2}}, "a3":{"b":null}}, "a4":{"c":null}}""";
      JsonValue jv = defaultJsonParser.createJsonValue(value);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key, jpMulti);
      assertThat(compareJSONGet(result, jv, jpMulti)).isEqualTo(true);
   }

   @Test
   public void testJSONSETArray() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("{}");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      jp = new JsonPath("$.foo");
      jv = defaultJsonParser.createJsonValue("[]");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      jp = new JsonPath("$.foo");
      jv = defaultJsonParser.createJsonValue("[0]");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      jp = new JsonPath("$.foo[1]");
      // Appending a value at the end of an array is allowed
      jv = defaultJsonParser.createJsonValue("[1]");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      // Appending a value "beyond" the end of an array raises exception
      JsonPath jp1 = new JsonPath("$.foo[3]");
      JsonValue jv1 = defaultJsonParser.createJsonValue("3");
      assertThatThrownBy(() -> {
         redis.jsonSet(key, jp1, jv1);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");
   }

   @Test
   public void testJSONSETEmptyArray() {
      String key = k();
      String value = """
            {"emptyArray":[]}""";
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue(value);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key);
      assertThat(result).hasSize(1);
      var resultStr = result.get(0).toString();
      assertThat(resultStr).isEqualTo(value);
   }

   @Test
   public void testJSONSETEmptyObject() {
      String key = k();
      String value = """
            {"emptyObject":{}}""";
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue(value);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key);
      assertThat(result).hasSize(1);
      var resultStr = result.get(0).toString();
      assertThat(resultStr).isEqualTo(value);
   }

   @Test
   public void testJSONSETUndefinitePathError() {
      String key = k();
      String value = """
            {}""";
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue(value);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      JsonPath jp1 = new JsonPath("$..f");
      JsonValue jv1 = defaultJsonParser.createJsonValue("1");
      assertThatThrownBy(() -> {
         redis.jsonSet(key, jp1, jv1);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");
      JsonPath jp2 = new JsonPath("$..[0]");
      JsonValue jv2 = defaultJsonParser.createJsonValue("2");
      assertThatThrownBy(() -> {
         redis.jsonSet(key, jp2, jv2);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");
   }

   @Test
   public void testJSONSETNegativeFloat() {
      String key = k();
      JsonValue jv = defaultJsonParser.createJsonValue("-1.2");

      JsonPath jp = new JsonPath("$");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(compareJSONGet(result, jv)).isEqualTo(true);

      JsonPath jpDot = new JsonPath(".");
      result = redis.jsonGet(key, jpDot);
      assertThat(compareJSONGet(result, jv, jpDot)).isEqualTo(true);
   }

   @Test
   public void testJSONGET() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("""
               { "key1":"value1",
                 "key2":"value2"
               }
            """);
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      JsonPath jp1 = new JsonPath("$.key1");
      var result = redis.jsonGet(k(), jp1);
      assertThat(compareJSONGet(result, jv, jp1)).isEqualTo(true);

      JsonPath jp2 = new JsonPath("$.key2");
      result = redis.jsonGet(k(), jp1, jp2);
      assertThat(compareJSONGet(result, jv, jp1, jp2)).isEqualTo(true);

      // One path multiple results
      JsonPath jp4 = new JsonPath("$.*");
      result = redis.jsonGet(k(), jp4);
      assertThat(compareJSONGet(result, jv, jp4)).isEqualTo(true);

      // Multiple path multiple results
      JsonPath jp5 = new JsonPath("$.key1");
      result = redis.jsonGet(k(), jp4, jp5);
      assertThat(compareJSONGet(result, jv, jp4, jp5)).isEqualTo(true);

      // Missing key
      jp5 = new JsonPath("$.key5");
      result = redis.jsonGet(k(), jp5);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).isJsonArray()).isTrue();
      assertThat(result.get(0).asJsonArray().asList()).hasSize(0);
      assertThat(compareJSONGet(result, jv, jp5)).isEqualTo(true);

      // Multiple path one missing key
      jp5 = new JsonPath("$.key5");
      result = redis.jsonGet(k(), jp5, jp2);
      assertThat(result).hasSize(1);
      assertThat(compareJSONGet(result, jv, jp5, jp2)).isEqualTo(true);
   }

   @Test
   public void testJSONGETLegacy() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("""
               { "key1":"value1",
                 "key2":["value2","value3"],
                 "key3":{"key4": "value4"},
                 "key4": null
               }
            """);
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      String p1 = ".key1";
      JsonPath jp1 = new JsonPath(p1);
      var result = defaultJsonParser.createJsonValue(command.jsonGet(k(), p1));
      // No need to wrap
      assertThat(compareJSONGet(result, jv, jp1)).isEqualTo(true);

      String p2 = ".key2";
      JsonPath jp2 = new JsonPath(p2);
      result = defaultJsonParser.createJsonValue(command.jsonGet(k(), p1, p2));
      // With legacy no need to wrap the root object in array
      assertThat(compareJSONGet(result, jv, jp1, jp2)).isEqualTo(true);

      String p3 = ".key3";
      JsonPath jp3 = new JsonPath(p3);
      result = defaultJsonParser.createJsonValue(command.jsonGet(k(), p1, p2, p3));
      assertThat(compareJSONGet(result, jv, jp1, jp2, jp3)).isEqualTo(true);

      result = defaultJsonParser.createJsonValue(command.jsonGet(k(), p3));
      assertThat(compareJSONGet(result, jv, jp3)).isEqualTo(true);

      // One path multiple results
      String p4 = ".*";
      JsonPath jp4 = new JsonPath(p4);
      result = defaultJsonParser.createJsonValue(command.jsonGet(k(), p4));
      assertThat(compareJSONGet(result, jv, jp4)).isEqualTo(true);

      // Multiple path multiple results
      String p5 = ".key1";
      JsonPath jp5 = new JsonPath(p5);
      result = defaultJsonParser.createJsonValue(command.jsonGet(k(), p4, p5));
      assertThat(compareJSONGet(result, jv, jp4, jp5)).isEqualTo(true);

      // Mixing legacy and not legacy path
      String p6 = ".key3";
      String p7 = ".key4";
      String p8 = ".key5";
      JsonPath jp6 = new JsonPath(p6);
      JsonPath jp7 = new JsonPath(p7);

      result = defaultJsonParser.createJsonValue(command.jsonGet(k(), p1, p2, p6));
      assertThat(compareJSONGet(result, jv, jp1, jp2, jp6)).isEqualTo(true);

      result = defaultJsonParser.createJsonValue(command.jsonGet(k(), p1, p2, p7));
      assertThat(compareJSONGet(result, jv, jp1, jp2, jp7)).isEqualTo(true);

      assertThatThrownBy(() -> {
         command.jsonGet(k(), p1, p2, p8);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");

      // Multiple path multiple results, legacy and not
      result = defaultJsonParser.createJsonValue(command.jsonGet(k(), p4, p6));
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
   public void testJSONSETWrongType() {
      String key = k();
      JsonValue jv = defaultJsonParser.createJsonValue("{\"key\":\"value\"}");
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
   public void testJSONGETPrettyPrinter() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("""
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
      JsonValue jv = defaultJsonParser.createJsonValue(value);
      JsonPath jp = new JsonPath("$");
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      List<JsonValue> result = redis.jsonGet(k(), new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSONGet(result, jv)).isTrue();
   }

   @Test
   public void testJSONOBJLEN() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      String jsonValue = """
      {
       "root":  {
             "name": "Example Name",
             "nested": {
                 "field1": "value1",
                 "field2": 42
             },
             "items": [
                 { "id": 1, "value": "Item One" },
                 { "id": 2, "value": "Item Two" },
                 { "id": 3, "value": "Item Three" }
             ]
         }
      }
         """;
      JsonValue jv = defaultJsonParser.createJsonValue(jsonValue);
      redis.jsonSet(key, jp, jv);
      var result = redis.jsonObjlen(key, jp);
      assertThat(result).containsExactly(1L);
      jp = new JsonPath("$.root");
      result = redis.jsonObjlen(key, jp);
      assertThat(result).containsExactly(3L);
      jp = new JsonPath("$.root.*");
      result = redis.jsonObjlen(key, jp);
      assertThat(result).containsExactly(null, 2L, null);
      // No path or old style path returns null on non existing object
      assertThat(redis.jsonObjlen("notExistingKey")).contains(new Long[]{null});
      jp = new JsonPath(".");
      assertThat(redis.jsonObjlen("notExistingKey", jp)).contains(new Long[]{null});
      // Jsonpath style returns error on non exsisting object. Cannot use
      // a simple "$" path, since lettuce doesn't pass it to the server
      assertThatThrownBy(() -> { redis.jsonObjlen("notExistingKey", new JsonPath("$.root"));
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");
   }

   // Lettuce Json object doesn't implement comparison. Implementing here
   private boolean compareJSONGet(JsonValue result, JsonValue expected, JsonPath... paths) {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode expectedObjectNode, resultNode;
      if (paths.length == 0) {
         paths = new JsonPath[] { new JsonPath("$") };
      }
      try {
         expectedObjectNode = (JsonNode) mapper.readTree(expected.toString());
         resultNode = (JsonNode) mapper.readTree(result.toString());
         var jpCtx = JSONUtil.parserForGet.parse(expectedObjectNode);
         boolean isLegacy = true;
         // If all paths are legacy, return results in legacy mode. i.e. no array
         for (JsonPath path : paths) {
            isLegacy &= !JSONUtil.isJsonPath(path.toString());
         }
         if (paths.length == 1) {
            // jpctx.read doesn't like legacy ".", change it to "$". everything else seems
            // to work
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

   private boolean compareJSONGet(List<JsonValue> results, JsonValue expected, JsonPath... paths) {
      assertThat(results).hasSize(1);
      return compareJSONGet(results.get(0), expected, paths);
   }

   private boolean compareJSONSet(JsonValue newRoot, JsonValue oldRoot, String path, JsonValue node) {
      ObjectMapper mapper = new ObjectMapper();
      try {
         // Unwrap objects if in an array
         var newRootNode = mapper.readTree(newRoot.toString());
         var oldRootNode = mapper.readTree(oldRoot.toString());
         var jpCtx = JSONUtil.parserForGet.parse(oldRootNode);
         var newNode = mapper.readTree(node.toString());
         com.jayway.jsonpath.JsonPath jPath = com.jayway.jsonpath.JsonPath.compile(path);
         if (jPath.isDefinite()) {
            jPath.set(jpCtx.json(), newNode, JSONUtil.configForDefiniteSet);
         } else {
            jPath.set(jpCtx.json(), newNode, JSONUtil.configForSet);
         }
         // Check the whole doc is correct
         if (!oldRootNode.equals(newRootNode)) {
            return false;
         }
         // Check the node is set correctly
         var newJpCtx = JSONUtil.parserForGet.parse(newRootNode);
         var newNodeFromNewDoc = newJpCtx.read(path);
         var expectedNode = jpCtx.read(path);
         return newNodeFromNewDoc.equals(expectedNode);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private boolean compareJSONSet(List<JsonValue> newRootList, JsonValue oldRoot, String path, JsonValue node) {
      assertThat(newRootList).hasSize(1);
      return compareJSONSet(newRootList.get(0), oldRoot, path, node);
   }
}
