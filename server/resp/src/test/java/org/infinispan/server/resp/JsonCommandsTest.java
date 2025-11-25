package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.server.resp.json.JSONUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.json.DefaultJsonParser;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.json.JsonType;
import io.lettuce.core.json.JsonValue;
import io.lettuce.core.json.arguments.JsonGetArgs;
import io.lettuce.core.json.arguments.JsonMsetArgs;
import io.lettuce.core.json.arguments.JsonRangeArgs;
import io.lettuce.core.json.arguments.JsonSetArgs;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.IntegerListOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.NumberListOutput;
import io.lettuce.core.output.StringListOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;

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
      JsonValue jvDoc = defaultJsonParser.createJsonValue("""
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
      }).isInstanceOf(RedisCommandExecutionException.class).hasMessageStartingWith("ERR ");
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
      }).isInstanceOf(RedisCommandExecutionException.class).hasMessageStartingWith("ERR ");
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
      }).isInstanceOf(RedisCommandExecutionException.class).hasMessageStartingWith("ERR ");
      JsonPath jp2 = new JsonPath("$..[0]");
      JsonValue jv2 = defaultJsonParser.createJsonValue("2");
      assertThatThrownBy(() -> {
         redis.jsonSet(key, jp2, jv2);
      }).isInstanceOf(RedisCommandExecutionException.class).hasMessageStartingWith("ERR ");
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
      }).isInstanceOf(RedisCommandExecutionException.class).hasMessageStartingWith("ERR ");

      // Multiple path multiple results, legacy and not
      result = defaultJsonParser.createJsonValue(command.jsonGet(k(), p4, p6));
      assertThat(compareJSONGet(result, jv, jp4, jp6)).isEqualTo(true);
   }

   @Test
   public void testJSONGETLegacyError() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      assertThatThrownBy(() -> {
         command.jsonSet(k(), "..", "{ \"k1\": \"v1\"}");
      }).isInstanceOf(RedisCommandExecutionException.class).hasMessageStartingWith("ERR ");
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
      // JSON.SET doc . {}
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("{}"))).isEqualTo("OK");
      // JSON.OBJTLEN doc
      assertThat(redis.jsonObjlen(key)).containsExactly(0L);

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
      jp = new JsonPath("$.nowhere");
      result = redis.jsonObjlen(key, jp);
      assertThat(result).isEmpty();
      // No path or old style path returns null on non-existing object
      assertThat(redis.jsonObjlen("notExistingKey").get(0)).isNull();
      jp = new JsonPath(".");
      assertThat(redis.jsonObjlen("notExistingKey", jp).get(0)).isNull();
      // Jsonpath style returns error on non-existing object. Cannot use
      // a simple "$" path, since lettuce doesn't pass it to the server
      assertThatThrownBy(() -> {
         redis.jsonObjlen("notExistingKey", new JsonPath("$.root"));
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR Path '$.root' does not exist or not an object");
   }

   @Test
   public void testJSONSTRLEN() {
      JsonPath jpDollar = new JsonPath("$");
      // JSON.STRLEN notExistingKey $
      assertThatThrownBy(() -> {
         RedisCodec<String, String> codec = StringCodec.UTF8;
         redis.dispatch(CommandType.JSON_STRLEN, new IntegerOutput<>(codec),
               new CommandArgs<>(codec).addKey("notExistingKey").add("$"));
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR could not perform this operation on a key that doesn't exist");
      // No path or old style path returns null on non-existing key
      // JSON.STRLEN notExistingKey
      assertThat(redis.jsonStrlen("notExistingKey").get(0)).isNull();
      // JSON.STRLEN notExistingKey .
      assertThat(redis.jsonStrlen("notExistingKey", new JsonPath(".")).get(0)).isNull();

      // Not a JSON
      redis.set("errorRaise", "world");
      // JSON.STRLEN errorRaise $
      assertThatThrownBy(() -> {
         redis.jsonStrlen("errorRaise", jpDollar);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

      String key = "doc";

      // JSON.SET doc . '"infinispan"'
      assertThat(redis.jsonSet(key, jpDollar, defaultJsonParser.createJsonValue("\"infinispan\""))).isEqualTo("OK");
      // JSON.STRLEN doc
      assertThat(redis.jsonStrlen(key)).containsExactly(10L);

      JsonValue jv = defaultJsonParser.createJsonValue("""
               {"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31}}
            """);

      // JSON.SET doc $ '{"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31}}'
      assertThat(redis.jsonSet(key, jpDollar, jv)).isEqualTo("OK");

      //  JSON.STRLEN doc .
      assertThatThrownBy(() ->
         redis.jsonStrlen(key, new JsonPath("."))
      ).isInstanceOf(RedisCommandExecutionException.class)
              .hasMessage("ERR Path '.' does not exist or not a string");

      //  JSON.STRLEN doc $..a
      assertThat(redis.jsonStrlen(key, new JsonPath("$..a"))).containsExactly(3L, 5L, null);
   }

   @Test
   public void testJSONARRLEN() {
      JsonPath jpDollar = new JsonPath("$");
      RedisCodec<String, String> codec = StringCodec.UTF8;
      JsonValue jv = defaultJsonParser.createJsonValue("""
               {
                   "name":"Wireless earbuds",
                   "description":"Wireless Bluetooth in-ear headphones",
                   "connection":{"wireless":true,"type":"Bluetooth"},
                   "price":64.99,"stock":17,
                   "colors":["black","white"],
                   "max_level":[80, 100, 120]
                }
            """);

      // JSON.ARRLEN notExistingKey $
      assertThatThrownBy(() -> {
         redis.dispatch(CommandType.JSON_ARRLEN, new IntegerOutput<>(codec),
               new CommandArgs<>(codec).addKey("notExistingKey").add("$"));
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR could not perform this operation on a key that doesn't exist");
      // No path or old style path returns null on non-existing key
      // JSON.ARRLEN notExistingKey
      assertThat(redis.jsonArrlen("notExistingKey").get(0)).isNull();
      // JSON.ARRLEN notExistingKey .
      assertThat(redis.jsonArrlen("notExistingKey", new JsonPath(".")).get(0)).isNull();

      // SET errorRaise world
      redis.set("errorRaise", "world");
      // JSON.ARRLEN errorRaise $
      assertThatThrownBy(() -> {
         redis.jsonArrlen("errorRaise", jpDollar);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

      // Test root elements
      // JSON.SET doc . []
      String key = "doc";
      assertThat(redis.jsonSet(key, jpDollar, defaultJsonParser.createJsonValue("[]"))).isEqualTo("OK");
      // JSON.ARRLEN doc
      assertThat(redis.jsonArrlen(key)).containsExactly(0L);
      // JSON.SET doc . '"hello"'
      assertThat(redis.jsonSet(key, jpDollar, defaultJsonParser.createJsonValue("\"hello\""))).isEqualTo("OK");
      // JSON.ARRLEN doc
      assertThatThrownBy(() ->
         redis.jsonArrlen(key)
      ).isInstanceOf(RedisCommandExecutionException.class)
              .hasMessage("ERR Path '.' does not exist or not an array");
      // JSON.SET doc . '{"v1": 2}'
      assertThat(redis.jsonSet(key, jpDollar, defaultJsonParser.createJsonValue("{\"v1\": 2}"))).isEqualTo("OK");
      // JSON.ARRLEN doc
      assertThatThrownBy(() ->
              redis.jsonArrlen(key)
      ).isInstanceOf(RedisCommandExecutionException.class)
              .hasMessage("ERR Path '.' does not exist or not an array");

      // JSON.SET doc $ '{"name":"Wireless earbuds","description":"Wireless Bluetooth in-ear headphones","connection":{"wireless":true,"type":"Bluetooth"},"price":64.99,"stock":17,"colors":["black","white"], "max_level":[80, 100, 120]}'
      assertThat(redis.jsonSet(key, jpDollar, jv)).isEqualTo("OK");
      assertThatThrownBy(() ->
              redis.jsonArrlen(key, new JsonPath("."))
      ).isInstanceOf(RedisCommandExecutionException.class)
              .hasMessage("ERR Path '.' does not exist or not an array");

      assertThatThrownBy(() ->
              redis.jsonArrlen(key, new JsonPath("..notExists"))
      ).isInstanceOf(RedisCommandExecutionException.class)
              .hasMessage("ERR Path '..notExists' does not exist");

      // JSON.ARRLEN doc $..max_level
      assertThat(redis.jsonArrlen(key, new JsonPath("$..max_level"))).containsExactly(3L);

      // JSON.ARRLEN doc $.[*]
      assertThat(redis.jsonArrlen(key, new JsonPath("$.[*]"))).containsExactly(null, null, null, null, null, 2L, 3L);
   }

   @Test
   public void testJSONTYPE() {
      JsonPath jp = new JsonPath("$");
      RedisCodec<String, String> codec = StringCodec.UTF8;
      String key = k();
      // JSON.SET doc . []
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("[]"))).isEqualTo("OK");
      // JSON.TYPE doc
      assertThat(redis.jsonType(key)).containsExactly(JsonType.ARRAY);
      // JSON.SET doc . '"hello json string"'
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("\"hello json string\""))).isEqualTo("OK");
      // JSON.TYPE doc
      assertThat(redis.jsonType(key)).containsExactly(JsonType.STRING);
      // JSON.SET doc . '1'
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("1"))).isEqualTo("OK");
      // JSON.TYPE doc
      assertThat(redis.jsonType(key)).containsExactly(JsonType.INTEGER);
      // JSON.SET doc . 'true'
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("true"))).isEqualTo("OK");
      // JSON.TYPE doc
      assertThat(redis.jsonType(key)).containsExactly(JsonType.BOOLEAN);
      // JSON.SET doc . '{}'
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("{}"))).isEqualTo("OK");
      // JSON.TYPE doc
      assertThat(redis.jsonType(key)).containsExactly(JsonType.OBJECT);
      // JSON.SET doc . '2.0'
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("2.0"))).isEqualTo("OK");
      // JSON.TYPE doc
      assertThat(redis.jsonType(key)).containsExactly(JsonType.NUMBER);

      JsonValue jv = defaultJsonParser.createJsonValue("""
               {"a":2,
               "null_value": null,
               "float_value": 12.3,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"a": true},
               "foo": "bar"}
            """);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      // Legacy: JSON.TYPE doc ..a
      assertThat(redis.dispatch(CommandType.JSON_TYPE, new ValueOutput<>(codec),
            new CommandArgs<>(codec).addKey(key).add("..a"))).isEqualTo("integer");
      // JSON.TYPE doc
      assertThat(redis.jsonType(key)).containsExactly(JsonType.OBJECT);
      // JSON.TYPE doc $..foo
      assertThat(redis.jsonType(key, new JsonPath("$..foo"))).containsExactly(JsonType.STRING);
      // JSON.TYPE doc $..null_value
      assertThat(redis.jsonType(key, new JsonPath("$..null_value"))).containsExactly(JsonType.UNKNOWN);
      assertThat(redis.dispatch(CommandType.JSON_TYPE, new StringListOutput<>(codec),
            new CommandArgs<>(codec).addKey(key).add("$..null_value"))).containsExactly("null");
      // JSON.TYPE doc $..a
      assertThat(redis.jsonType(key, new JsonPath("$..a"))).containsExactly(JsonType.INTEGER, JsonType.BOOLEAN);
      // JSON.TYPE doc $..float_value
      assertThat(redis.jsonType(key, new JsonPath("$..float_value"))).containsExactly(JsonType.NUMBER);
      // JSON.TYPE doc $..arr_value
      assertThat(redis.jsonType(key, new JsonPath("$..arr_value"))).containsExactly(JsonType.ARRAY);
      // JSON.TYPE doc $..dummy
      assertThat(redis.jsonType(key, new JsonPath("$..dummy"))).isEmpty();
      // JSON.TYPE notExistingKey
      assertThat(redis.dispatch(CommandType.JSON_TYPE, new IntegerOutput<>(codec),
            new CommandArgs<>(codec).addKey("notExistingKey"))).isNull();
   }

   public void testJSONDEL() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      String jsonStr = """
               {
                  "name": "Alice",
                  "age": 30,
                  "isStudent": false,
                  "grades": [85, 90, 78],
                  "address": {
                    "verified": true,
                    "city": "New York",
                    "zip": "10001"
                  },
                  "phone": null
                }
            """;
      JsonValue jv = defaultJsonParser.createJsonValue(jsonStr);
      redis.jsonSet(key, jp, jv);
      // Deleting whole object
      assertThat(redis.jsonDel(key, jp)).isEqualTo(1);
      // Not using json.get since lettuce json.get fails with RESP3 null
      assertThat(redis.get(key)).isNull();
      redis.jsonSet(key, jp, jv);
      // Deleting nested element
      jp = new JsonPath("$.address.city");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(1);
      // Deleting multiple elements
      jp = new JsonPath("$.address.*");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(2);
      // Deleting null element
      jp = new JsonPath("$.phone");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(1);
      // Deleting array element
      jp = new JsonPath("$.grades[2]");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(1);
      List<JsonValue> jsonGetResult = redis.jsonGet(key);
      assertThat(jsonGetResult).hasSize(1);
      // Deleting non existing array element
      jp = new JsonPath("$.grades[3]");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(0);
      // Deleting all root element
      jp = new JsonPath("$.*");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(5);
      // Chech that the entry contains an empty json object
      jsonGetResult = redis.jsonGet(key);
      assertThat(jsonGetResult).hasSize(1);
      assertThat(jsonGetResult.get(0).toString()).isEqualTo("{}");
      // Repeat test with legacy path
      jp = new JsonPath(".");
      jv = defaultJsonParser.createJsonValue(jsonStr);
      redis.jsonSet(key, jp, jv);
      // Deleting whole object
      assertThat(redis.jsonDel(key, jp)).isEqualTo(1);
      // Not using json.get since lettuce json.get fails with RESP3 null
      assertThat(redis.get(key)).isNull();
      redis.jsonSet(key, jp, jv);
      // Deleting nested element
      jp = new JsonPath(".address.city");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(1);
      // Deleting multiple elements
      jp = new JsonPath(".address.*");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(2);
      // Deleting null element
      jp = new JsonPath(".phone");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(1);
      // Deleting array element
      jp = new JsonPath(".grades[2]");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(1);
      jsonGetResult = redis.jsonGet(key);
      assertThat(jsonGetResult).hasSize(1);
      // Deleting non existing array element
      jp = new JsonPath(".grades[3]");
      assertThat(redis.jsonDel(key, jp)).isEqualTo(0);
   }

   @Test
   public void testJSONFORGET() {
      // JSON.FORGET is an alias for JSON.DEL. Check if alias is in place
      String key = k();
      String jsonStr = """
               {
                  "name": "Alice",
                  "age": 30,
                  "isStudent": false,
                  "grades": [85, 90, 78],
                  "address": {
                    "verified": true,
                    "city": "New York",
                    "zip": "10001"
                  },
                  "phone": null
                }
            """;
      JsonValue jv = defaultJsonParser.createJsonValue(jsonStr);
      redis.jsonSet(key, new JsonPath("$"), jv);
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      // Deleting whole object
      assertThat(command.jsonForget(key, "$")).isEqualTo(1);
      // Not using json.get since lettuce json.get fails with RESP3 null
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testJSONSTRAPPEND() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("\"string\"");
      String key = k();
      // Append to root
      redis.jsonSet(key, jp, jv);
      JsonValue append = defaultJsonParser.fromObject("Append");
      assertThat(redis.jsonStrappend(key, jp, append)).containsExactly(12L);
      List<JsonValue> jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[\"stringAppend\"]");
      jv = defaultJsonParser.createJsonValue("""
               {"a":2,
               "null_value": null,
               "float_value": 12.3,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"a": true,
                  "foo": false,
                  "nested2": {
                   "foo": "fore"}},
               "foo": "bar"}
            """);
      key = k(1);
      redis.jsonSet(key, jp, jv);
      // Append to single element
      jp = new JsonPath("$.foo");
      assertThat(redis.jsonStrappend(key, jp, append)).containsExactly(9L);
      jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[\"barAppend\"]");
      // Append to multiple elements
      jp = new JsonPath("$..foo");
      assertThat(redis.jsonStrappend(key, jp, append)).containsExactly(15L, null, 10L);
      // Append to nonstring element
      jp = new JsonPath("$.float_value");
      List<Long> jsonStrappend = redis.jsonStrappend(key, jp, append);
      assertThat(jsonStrappend).containsExactly((Long) null);

      // Test legacy path
      // Append to root
      jp = new JsonPath(".");
      JsonValue jv1 = defaultJsonParser.createJsonValue("\"string\"");
      redis.jsonSet(key, jp, jv1);
      append = defaultJsonParser.fromObject("Append");
      assertThat(redis.jsonStrappend(key, jp, append)).containsExactly(12L);
      jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("\"stringAppend\"");
      key = k(1);
      redis.jsonSet(key, jp, jv);
      // Append to single element
      jp = new JsonPath(".foo");
      assertThat(redis.jsonStrappend(key, jp, append)).containsExactly(9L);
      jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("\"barAppend\"");
      // Append to nonstring element
      jp = new JsonPath(".float_value");
      var keyFinal = key;
      var jpFinal = jp;
      var appendFinal = append;
      assertThatThrownBy(() -> {
         redis.jsonStrappend(keyFinal, jpFinal, appendFinal);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR Path '$.float_value' does not exist or not a string");
      assertThat(jsonStrappend).containsExactly((Long) null);
   }

   @Test
   public void testJSONARRAPPEND() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("[1, \"a\", {\"o\":\"v\"}]");
      String key = k();
      // Append to root
      redis.jsonSet(key, jp, jv);
      List<JsonValue> arr = redis.jsonGet(key, jp);
      assertThat(arr).isNotNull();
      Long res = command.jsonArrappend(key, "$", "\"aString\"", "1", "{\"aObj\": null}");
      assertThat(res).isEqualTo(6);
      List<JsonValue> jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[[1,\"a\",{\"o\":\"v\"},\"aString\",1,{\"aObj\":null}]]");
      jv = defaultJsonParser.createJsonValue("""
               {"a": 2,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"a": true,
                  "nested2": {
                   "foo": "fore"},
                   "arr_value": [1,2,3,4]
                  },
               "nested1":
                  {
                   "arr_value": null
                  },
               "nested2":
                  {
                   "arr_value": 1
                  }
               }
            """);
      key = k(1);
      redis.jsonSet(key, jp, jv);
      // Append to single element
      jp = new JsonPath("$.nested.arr_value");
      JsonValue app1 = defaultJsonParser.fromObject("aString");
      JsonValue app2 = defaultJsonParser.fromObject(1);
      JsonValue app3 = defaultJsonParser.createJsonValue("{\"aObj\": null}");
      List<Long> jsonArrappend = redis.jsonArrappend(key, jp, app1, app2, app3);
      assertThat(jsonArrappend).containsExactly(7L);
      jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[[1,2,3,4,\"aString\",1,{\"aObj\":null}]]");
      // Append to multiple elements

      jp = new JsonPath("$..arr_value");
      assertThat(redis.jsonArrappend(key, jp, app1, app2, app3)).containsExactly(6L, 10L, null, null);

      // Test legacy path
      command = CustomStringCommands.instance(redisConnection);
      jp = new JsonPath("$");
      jv = defaultJsonParser.createJsonValue("[1, \"a\", {\"o\":\"v\"}]");
      key = k();
      // Append to root
      redis.jsonSet(key, jp, jv);
      arr = redis.jsonGet(key, jp);
      assertThat(arr).isNotNull();
      res = command.jsonArrappend(key, ".", "\"aString\"", "1", "{\"aObj\": null}");
      assertThat(res).isEqualTo(6);
      jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[[1,\"a\",{\"o\":\"v\"},\"aString\",1,{\"aObj\":null}]]");
      jv = defaultJsonParser.createJsonValue("""
               {"a": 2,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"a": true,
                  "nested2": {
                   "foo": "fore"},
                   "arr_value": [1,2,3,4]
                  },
               "nested1":
                  {
                   "arr_value": null
                  },
               "nested2":
                  {
                   "arr_value": 1
                  }
               }
            """);
      key = k(1);
      redis.jsonSet(key, jp, jv);
      // Append to single element
      jp = new JsonPath(".nested.arr_value");
      app1 = defaultJsonParser.fromObject("aString");
      app2 = defaultJsonParser.fromObject(1);
      app3 = defaultJsonParser.createJsonValue("{\"aObj\": null}");
      assertThat(redis.jsonArrappend(key, jp, app1, app2, app3)).containsExactly(7L);
      jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[1,2,3,4,\"aString\",1,{\"aObj\":null}]");
   }

   @Test
   public void testJSONTOGGLE() {
      JsonPath jp = new JsonPath("$");
      RedisCodec<String, String> codec = StringCodec.UTF8;
      String key = k();

      // JSON.SET doc $ 'true'
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("true"))).isEqualTo("OK");
      assertThat(redis.jsonGet(key).get(0).asBoolean()).isTrue();
      assertThat(redis.dispatch(CommandType.JSON_TOGGLE, new ValueOutput<>(codec),
              new CommandArgs<>(codec).addKey(key).add("."))).isEqualTo("false");
      assertThat(redis.jsonGet(key).get(0).asBoolean()).isFalse();

      JsonValue jv = defaultJsonParser.createJsonValue("""
               {"bool":true,
               "null_value": null,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"bool": false},
               "foo": "bar",
               "legacy": true}
            """);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");

      // Legacy
      // JSON.TOGGLE doc ..legacy
      assertThat(redis.dispatch(CommandType.JSON_TOGGLE, new ValueOutput<>(codec),
            new CommandArgs<>(codec).addKey(key).add("..legacy"))).isEqualTo("false");
      // JSON.TOGGLE doc ..legacy
      assertThat(redis.dispatch(CommandType.JSON_TOGGLE, new ValueOutput<>(codec),
            new CommandArgs<>(codec).addKey(key).add("..legacy"))).isEqualTo("true");
      // JSON.TOGGLE doc ..notExistingPathLegacy
      assertThatThrownBy(() -> redis.dispatch(CommandType.JSON_TOGGLE, new ValueOutput<>(codec),
            new CommandArgs<>(codec).addKey(key).add("..notExistingPathLegacy")))
                  .isInstanceOf(RedisCommandExecutionException.class)
                  .hasMessage("ERR Path '$..notExistingPathLegacy' does not exist or not a bool");

      // JSON.TOGGLE doc $..bool
      assertThat(redis.jsonToggle(key, new JsonPath("$..bool"))).containsExactly(0L, 1L);
      // JSON.TOGGLE doc $..bool
      assertThat(redis.jsonToggle(key, new JsonPath("$..bool"))).containsExactly(1L, 0L);
      // JSON.TOGGLE doc $.bool
      assertThat(redis.jsonToggle(key, new JsonPath("$.bool"))).containsExactly(0L);

      // JSON.TOGGLE doc $..notExistingPath
      assertThat(redis.jsonToggle(key, new JsonPath("$..notExistingPath"))).isEmpty();

      // JSON.TOGGLE doc $..null_value
      assertThat(redis.jsonToggle(key, new JsonPath("$..null_value")).get(0)).isNull();

      // JSON.TOGGLE notExistingKey $..value
      assertThatThrownBy(() -> redis.jsonToggle("notExistingKey", new JsonPath("$..value")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR could not perform this operation on a key that doesn't exist");
   }

   @Test
   public void testJSONOBJKEYS() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("""
               {"bool":true,
               "null_value": null,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"bool": false,
                   "stringKey": "aString"},
               "foo": "bar",
               "legacy": true}
            """);
      String key = k();
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      jp = new JsonPath("$");
      List<String> result = redis.jsonObjkeys(key, jp);
      assertThat(result).containsExactly("bool", "null_value", "arr_value", "nested", "foo", "legacy");
      jp = new JsonPath("$.bool");
      result = redis.jsonObjkeys(key, jp);
      assertThat(result).containsExactly((String) null);
      jp = new JsonPath("$.nested");
      result = redis.jsonObjkeys(key, jp);
      assertThat(result).containsExactly("bool", "stringKey");
      jp = new JsonPath("$.*");
      result = redis.jsonObjkeys(key, jp);
      assertThat(result).containsExactly(null, null, null, "bool", "stringKey", null, null);
      jp = new JsonPath("$.non_existing");
      result = redis.jsonObjkeys(key, jp);
      assertThat(result).isEmpty();
      jp = new JsonPath("$");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");

      // Legacy path testing
      jp = new JsonPath(".");
      result = redis.jsonObjkeys(key, jp);
      assertThat(result).containsExactly("bool", "null_value", "arr_value", "nested", "foo", "legacy");
      JsonPath jp1 = new JsonPath(".bool");
      assertThatThrownBy(() -> redis.jsonObjkeys(key, jp1)).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR Path '$.bool' does not exist or not an object");
      jp = new JsonPath("$.non_existing");
      result = redis.jsonObjkeys(key, jp);
      assertThat(result).isEmpty();
   }

   @Test
   public void testJSONNUMINCRBY() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("""
              {"a":"b", "b": [{"a":2}, {"a":"c"}, {"a":5.4}, {"a":true}, {"a":["hello"]}]}
            """);
      String key = k();
      // JSON.SET doc $ '1'
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("1"))).isEqualTo("OK");
      // JSON.NUMINCRBY doc $ 2 or JSON.NUMINCRBY doc . 2
      assertThat(redis.jsonNumincrby(key, jp, 2)).containsExactly(3L);
      // JSON.GET doc
      assertThat(redis.jsonGet(key).get(0).isNumber()).isTrue();
      assertThat(redis.jsonGet(key).get(0).asNumber().intValue()).isEqualTo(3);

      // JSON.SET doc $ '{"a":"b", "b": [{"a":2}, {"a":"c"}, {"a":5.4}, {"a":true},{"h":1}, {"a":["hello"]}]}'
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      //JSON.NUMINCRBY doc $..a 2
      assertThat(redis.jsonNumincrby(key, new JsonPath("$..a"), 2)).containsExactly(null, 4L, null, 7.4, null, null);
      //JSON.NUMINCRBY doc $..a 2.4
      assertThat(redis.jsonNumincrby(key, new JsonPath("$..a"), 2.4)).containsExactly(null, 6.4, null, 9.8, null, null);
      //JSON.NUMINCRBY doc $..b 12
      List<Number> bPathNumbers = redis.jsonNumincrby(key, new JsonPath("$..b"), 22);
      assertThat(bPathNumbers).hasSize(1);
      assertThat(bPathNumbers.get(0)).isNull();
      //JSON.NUMINCRBY doc $..notExisting 12.0
      assertThat(redis.jsonNumincrby(key, new JsonPath("$..notExisting"), 12.0)).isEmpty();
      //JSON.NUMINCRBY doc ..notExisting 12.0
      assertThat(redis.jsonNumincrby(key, new JsonPath("..notExisting"), 12.0)).isEmpty();
      //JSON.NUMINCRBY doc .notExisting 12.0
      assertThat(redis.jsonNumincrby(key, new JsonPath(".notExisting"), 12.0)).isEmpty();

      // JSON.NUMINCRBY notExistingKey $..value
      assertThatThrownBy(() -> redis.jsonNumincrby("notExistingKey", new JsonPath("$..a"), 2))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR could not perform this operation on a key that doesn't exist");
   }

   @Test
   public void testJSONNUMMULTRBY() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("""
              {"a":"b", "b": [{"a":2}, {"a":"c"}, {"a":5.4}, {"a":true}, {"a":["hello"]}]}
            """);
      String key = k();
      // JSON.SET doc $ '{"a":"b", "b": [{"a":2}, {"a":"c"}, {"a":5.4}, {"a":true},{"h":1}, {"a":["hello"]}]}'
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      // JSON.NUMMULTBY doc $..a 2
      assertThat(jsonMultBy(key, "$..a", 2)).containsExactly(null, 4L, null, 10.8, null, null);

      assertThatThrownBy(() ->
              jsonMultBy("notExistingKey", "$", 2))
              .isInstanceOf(RedisCommandExecutionException.class)
              .hasMessage("ERR could not perform this operation on a key that doesn't exist");
   }

   private List<Number> jsonMultBy(String key, String path, int multiplier) {
      RedisCodec<String, String> codec = StringCodec.UTF8;
      return redis.dispatch(TestCommandType.JSON_NUMMULTBY, new NumberListOutput<>(codec),
              new CommandArgs<>(codec).addKey(key).add(path).add(multiplier));
   }

   enum TestCommandType implements ProtocolKeyword {
      JSON_NUMMULTBY("JSON.NUMMULTBY"),
      JSON_DEBUG("JSON.DEBUG");

      public final byte[] bytes;
      private final String command;

      TestCommandType(String name) {
         this.command = name;
         this.bytes = name.getBytes(StandardCharsets.US_ASCII);
      }

      public String toString() {
         return this.command;
      }

      public byte[] getBytes() {
         return this.bytes;
      }
   }


   public void testJSONARRINDEX() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("""
               {"bool":true,
               "null_value": null,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"bool": false,
                   "arr_value": ["one", "three","four"],
                   "stringKey": "aString"},
               "nested1":
                  {"bool": false,
                   "arr_value": "not an array",
                   "stringKey": "aString"},
               "foo": "bar",
               "legacy": true}
            """);
      String key = k();
      redis.jsonSet(key, jp, jv);
      jp = new JsonPath("$.arr_value");
      JsonValue jv1 = defaultJsonParser.createJsonValue("\"three\"");
      List<Long> result = redis.jsonArrindex(key, jp, jv1);
      assertThat(result).containsExactly(2L);

      // Test args conditions
      result = redis.jsonArrindex(key, jp, jv1, JsonRangeArgs.Builder.start(0).stop(-1));
      assertThat(result).containsExactly(-1L);
      result = redis.jsonArrindex(key, jp, jv1, JsonRangeArgs.Builder.start(1).stop(0));
      assertThat(result).containsExactly(2L);
      result = redis.jsonArrindex(key, jp, jv1, JsonRangeArgs.Builder.start(0).stop(10));
      assertThat(result).containsExactly(2L);
      result = redis.jsonArrindex(key, jp, jv1, JsonRangeArgs.Builder.start(0).stop(10));
      assertThat(result).containsExactly(2L);
      result = redis.jsonArrindex(key, jp, jv1, JsonRangeArgs.Builder.start(5).stop(1));
      assertThat(result).containsExactly(-1L);

      // Multiple matches
      jp = new JsonPath("$..arr_value");
      result = redis.jsonArrindex(key, jp, jv1);
      assertThat(result).containsExactly(2L, 1L, null);
      result = redis.jsonArrindex(key, jp, jv1, JsonRangeArgs.Builder.start(0).stop(-1));
      assertThat(result).containsExactly(-1L, 1L, null);

      // Key non existent, path non existent
      JsonPath jp1 = new JsonPath("$.bool");
      JsonValue jv2 = jv1;
      assertThatThrownBy(() -> redis.jsonArrindex("non-existent", jp1, jv2))
            .isInstanceOf(RedisCommandExecutionException.class).hasMessage("ERR Path '$.bool' does not exist");
      JsonPath jp2 = new JsonPath("$.non-existent");
      assertThat(redis.jsonArrindex(key, jp2, jv1)).isEmpty();

      // Test legacy path
      jp = new JsonPath("$.arr_value");
      jv1 = defaultJsonParser.createJsonValue("\"three\"");
      result = redis.jsonArrindex(key, jp, jv1);
      assertThat(result).containsExactly(2L);
      JsonPath jp3 = new JsonPath(".non-existent");
      jp = new JsonPath("..arr_value");
      result = redis.jsonArrindex(key, jp, jv1);
      assertThat(result).containsExactly(2L);
      result = redis.jsonArrindex(key, jp, jv1, JsonRangeArgs.Builder.start(0).stop(-1));
      assertThat(result).containsExactly(-1L);
      assertThatThrownBy(() -> redis.jsonArrindex(key, jp3, jv2)).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR Path '$.non-existent' does not exist");
   }
   @Test
   public void testJSONARRPOP() {
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("""
               ["one", "two", "three","four"]
            """);
      String key = k();
      redis.jsonSet(key, jpRoot, jv);
      List<JsonValue> result = redis.jsonArrpop(key);
      assertThat(result.toString()).isEqualTo("[\"four\"]");
      result = redis.jsonArrpop(key, jpRoot, 0);
      assertThat(result.toString()).isEqualTo("[\"one\"]");
      result = redis.jsonArrpop(key, jpRoot, -1);
      assertThat(result.toString()).isEqualTo("[\"three\"]");
      result = redis.jsonArrpop(key, jpRoot, 1);
      assertThat(result.toString()).isEqualTo("[\"two\"]");
      // Not testing pop from empty array, bug in lettuce?
      // see https://github.com/redis/lettuce/issues/3196
      // result = redis.jsonArrpop(key, jp, 1);
      // assertThat(result1).isNull();
      // result = redis.jsonGet(key, jp);
      // assertThat(result.toString()).isEqualTo("[[[]]]");

      jv = defaultJsonParser.createJsonValue("""
               {"bool":true,
               "null_value": null,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"bool": false,
                   "arr_value": [1, 2, 3, 4],
                   "stringKey": "aString"},
               "foo": "bar",
               "legacy": true}
            """);
      redis.jsonSet(key, jpRoot, jv);
      JsonPath jp = new JsonPath("$..arr_value");
      result = redis.jsonArrpop(key, jp , 1);
      assertThat(result.toString()).isEqualTo("[\"two\", 2]");
      result = redis.jsonArrpop(key, jp , -1);
      assertThat(result.toString()).isEqualTo("[\"three\", 4]");
      result = redis.jsonArrpop(key, jp , 0);
      assertThat(result.toString()).isEqualTo("[\"one\", 1]");

      // Test legacy path
      redis.jsonSet(key, jpRoot, jv);
      jp = new JsonPath("..arr_value");
      result = redis.jsonArrpop(key, jp , 1);
      assertThat(result.toString()).isEqualTo("[2]");
      result = redis.jsonArrpop(key, jp , -1);
      assertThat(result.toString()).isEqualTo("[4]");
      result = redis.jsonArrpop(key, jp , 0);
      assertThat(result.toString()).isEqualTo("[1]");

      // Test pop on non-array element (JSONPath - should return array with null)
      redis.jsonSet(key, jpRoot, jv);
      JsonPath jpNonArray = new JsonPath("$.foo");
      result = redis.jsonArrpop(key, jpNonArray, 0);
      assertThat(result).hasSize(1);
      assertThat(result.toString()).isEqualTo("[null]");

      // Test pop on non-array element (legacy path - should also throw exception)
      JsonPath jpLegacyNonArray = new JsonPath(".foo");
      assertThatThrownBy(() -> redis.jsonArrpop(key, jpLegacyNonArray, 0))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR Path '$.foo' does not exist or not an array");
   }

   @Test
   public void testJSONARRTRIM() {
      var command = CustomStringCommands.instance(redisConnection);
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("""
               {"bool":true,
               "null_value": null,
               "arr_value": ["one", "two", "three", "four"],
               "nested":
                  {"bool": false,
                   "arr_value": [1,2,3],
                   "stringKey": "aString"},
               "nested1":
                  {"bool": false,
                   "arr_value": "not an array",
                   "stringKey": "aString"},
               "foo": "bar",
               "legacy": true}
            """);
      String key = k();
      redis.jsonSet(key, jpRoot, jv);
      JsonPath jp = new JsonPath("$.arr_value");
      List<Long> result = redis.jsonArrtrim(key, jp, JsonRangeArgs.Builder.start(0).stop(1));
      assertThat(result).containsExactly(2L);
      assertThat(redis.jsonGet(key, jp).get(0).toString()).isEqualTo("[[\"one\",\"two\"]]");

      // start > arraysize
      redis.jsonSet(key, jpRoot, jv);
      Long result1 = command.jsonArrtrim(key, "$.arr_value", 4, 5);
      assertThat(result1).isEqualTo(0L);
      List<JsonValue> jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[[]]");

      // start > stop
      redis.jsonSet(key, jpRoot, jv);
      result1 = (Long)command.jsonArrtrim(key, "$.arr_value", 1, 0);
      assertThat(result1).isEqualTo(0L);
      assertThat(redis.jsonGet(key, jp).get(0).toString()).isEqualTo("[[]]");

      // start and stop < 0
      redis.jsonSet(key, jpRoot, jv);
      result = redis.jsonArrtrim(key, jp, JsonRangeArgs.Builder.start(-2).stop(-1));
      assertThat(result).containsExactly(2L);
      assertThat(redis.jsonGet(key, jp).get(0).toString()).isEqualTo("[[\"three\",\"four\"]]");

      // stop > size
      redis.jsonSet(key, jpRoot, jv);
      result = redis.jsonArrtrim(key, jp, JsonRangeArgs.Builder.start(2).stop(5));
      assertThat(result).containsExactly(2L);
      assertThat(redis.jsonGet(key, jp).get(0).toString()).isEqualTo("[[\"three\",\"four\"]]");

      // Multiple match
      redis.jsonSet(key, jpRoot, jv);
      jp = new JsonPath("$..arr_value");
      result = redis.jsonArrtrim(key, jp, JsonRangeArgs.Builder.start(2).stop(4));
      assertThat(result).containsExactly(2L,1L,null);
      List<JsonValue> jsonGet2 = redis.jsonGet(key, jp).get(0).asJsonArray().asList();
      assertThat(jsonValueListToStringList(jsonGet2)).containsExactly("[\"three\",\"four\"]","[3]", "\"not an array\"");

      // Non existing key
      assertThatThrownBy(() -> redis.jsonArrtrim("non-existent", new JsonPath("$.arr_value"), JsonRangeArgs.Builder.start(0).stop(1)))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR could not perform this operation on a key that doesn't exist");

      // Non existing path
      assertThat(redis.jsonArrtrim(key, new JsonPath("$.non-existent"), JsonRangeArgs.Builder.start(0).stop(1))).isEmpty();

      // Test legacy path
      redis.jsonSet(key, jpRoot, jv);
      jp = new JsonPath("..arr_value");
      result = redis.jsonArrtrim(key, jp, JsonRangeArgs.Builder.start(2).stop(4));
      assertThat(result).containsExactly(1L);
      JsonValue jsonGet3 = redis.jsonGet(key, jp).get(0);
      assertThat(jsonGet3.toString()).isEqualTo("[\"three\",\"four\"]");

      // Non existing path with legacy
      assertThatThrownBy(() -> redis.jsonArrtrim(key, new JsonPath(".non-existing"), JsonRangeArgs.Builder.start(0).stop(1)))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR Path '$.non-existing' does not exist or not an array");
   }

   // A function that receive a List<JsonValue> and a return a List<String>
   private static List<String> jsonValueListToStringList(List<JsonValue> jsonValues) {
      return jsonValues.stream().map(JsonValue::toString).collect(Collectors.toList());
   }
   @Test
   public void testJSONCLEAR() {
      JsonPath jp = new JsonPath("$");

      String key = k();
      // JSON.SET doc $ 3
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("3"))).isEqualTo("OK");
      // JSON.GET doc
      assertThat(redis.jsonGet(key).get(0).asNumber()).isEqualTo(3);
      // JSON.CLEAR doc
      assertThat(redis.jsonClear(key)).isEqualTo(1L);
      // JSON.GET doc
      assertThat(redis.jsonGet(key).get(0).asNumber()).isEqualTo(0);

      // JSON.SET doc $ '["hello", "world"]'
      JsonValue arrayJson = defaultJsonParser.createJsonValue("""
              ["hello", "world"]""");
      assertThat(redis.jsonSet(key, jp, arrayJson)).isEqualTo("OK");
      assertThat(redis.jsonGet(key).get(0).asJsonArray().size()).isEqualTo(2);
      assertThat(redis.jsonClear(key)).isEqualTo(1L);
      assertThat(redis.jsonGet(key).get(0).asJsonArray().size()).isZero();

      // JSON.SET doc $ '"infinispan"'
      assertThat(redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("\"infinispan\""))).isEqualTo("OK");
      assertThat(redis.jsonGet(key).get(0).asString()).isEqualTo("infinispan");
      assertThat(redis.jsonClear(key)).isZero();
      assertThat(redis.jsonGet(key).get(0).asString()).isEqualTo("infinispan");

      JsonValue jv = defaultJsonParser.createJsonValue("""
                {"a":"b", "b": [{"a":2}, {"a":"c"}, {"a":5}, {"a":true}, {"a":{"h": 10}}, {"a":["hello"]}]}
              """);

      // JSON.SET doc $ '{"a":"b", "b": [{"a":2}, {"a":"c"}, {"a":5}, {"a":true}, {"a":{"h": 10}}, {"a":["hello"]}]}'
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      // JSON.CLEAR doc
      assertThat(redis.jsonClear(key)).isEqualTo(1L);
      // JSON.GET doc
      List<JsonValue> jsonValues = redis.jsonGet(key);
      assertThat(jsonValues).hasSize(1);
      assertThat(jsonValues.get(0).asJsonObject().toString()).isEqualTo("{}");

      // JSON.SET doc $ '{"a":"b", "b": [{"a":2}, {"a":"c"}, {"a":5}, {"a":true}, {"a":{"h": 10}}, {"a":["hello"]}]}'
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      // JSON.CLEAR doc $..noexistpath
      assertThat(redis.jsonClear(key, new JsonPath("$..noexistpath"))).isZero();
      // JSON.CLEAR doc $..a
      assertThat(redis.jsonClear(key, new JsonPath("$..a"))).isEqualTo(4L);
      // JSON.GET doc
      String afterClearJson = """
              {"a":"b","b":[{"a":0},{"a":"c"},{"a":0},{"a":true},{"a":{}},{"a":[]}]}""";
      assertThat(redis.jsonGet(key).get(0).asJsonObject().toString()).isEqualTo(afterClearJson);
      // JSON.CLEAR doc $..a
      assertThat(redis.jsonClear(key, new JsonPath("$..a"))).isZero();
      // JSON.GET doc
      assertThat(redis.jsonGet(key).get(0).asJsonObject().toString()).isEqualTo(afterClearJson);

      // JSON.CLEAR notExistingKey
      assertThatThrownBy(() ->
              redis.jsonClear("notExistingKey"))
              .isInstanceOf(RedisCommandExecutionException.class)
              .hasMessage("ERR could not perform this operation on a key that doesn't exist");
   }
   public void testJSONMERGE() {
      var command = CustomStringCommands.instance(redisConnection);
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("""
                     {
              "name": "Example Object",
              "id": 123,
              "obj1": {
                "type": "Nested Object",
                "status": "active",
                "nestedObj": {
                  "level": 2,
                  "description": "This is a deeply nested object"
                }
              }
            }
                     """);

      // Test create field
      String update = """
            {"obj1":{"nestedObj":{"added":"field"}}}
               """;
      // //JsonValue jvUpdate = defaultJsonParser.createJsonValue(update);
      String key = k();
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");
      assertThat(command.jsonMerge(key, "$", update)).isEqualTo("OK");
      List<JsonValue> result = redis.jsonGet(key, jpRoot);
      assertThat(result.toString()).isEqualTo(
            """
                  [[{"name":"Example Object","id":123,"obj1":{"type":"Nested Object","status":"active","nestedObj":{"level":2,"description":"This is a deeply nested object","added":"field"}}}]]""");

      // test delete field
      update = """
            { "name": "Example Object", "id": 123, "obj1": {"nestedObj": null}} }
            """;
      assertThat(command.jsonMerge(key, "$", update)).isEqualTo("OK");
      result = redis.jsonGet(key, jpRoot);
      assertThat(result.toString()).isEqualTo("""
            [[{"name":"Example Object","id":123,"obj1":{"type":"Nested Object","status":"active"}}]]""");

      // test replace field
      update = """
            { "name": "Example Object", "id": 123, "obj1": {"status": true}} }
            """;
      assertThat(command.jsonMerge(key, "$", update)).isEqualTo("OK");
      result = redis.jsonGet(key, jpRoot);
      assertThat(result.toString()).isEqualTo("""
            [[{"name":"Example Object","id":123,"obj1":{"type":"Nested Object","status":true}}]]""");

      // test merge with null
      update = "null";
      JsonValue jvUpdate = defaultJsonParser.createJsonValue(update);
      assertThat(redis.jsonMerge(key, new JsonPath("$.obj1"), jvUpdate)).isEqualTo("OK");
      result = redis.jsonGet(key, new JsonPath("$.obj1"));
      assertThat(result.toString()).isEqualTo("[[null]]");

      // Test create field on non root
      update = """
            {"added":"field"}
               """;
      jvUpdate = defaultJsonParser.createJsonValue(update);
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");
      assertThat(redis.jsonMerge(key,  new JsonPath("$.obj1.nestedObj"), jvUpdate)).isEqualTo("OK");
      result = redis.jsonGet(key, jpRoot);
      assertThat(result.toString()).isEqualTo(
            """
            [[{"name":"Example Object","id":123,"obj1":{"type":"Nested Object","status":"active","nestedObj":{"level":2,"description":"This is a deeply nested object","added":"field"}}}]]""");

      // Test delete field on non root
      update = """
            {"added":null}
               """;
      jvUpdate = defaultJsonParser.createJsonValue(update);
      assertThat(redis.jsonMerge(key, new JsonPath("$.obj1.nestedObj"), jvUpdate)).isEqualTo("OK");
      result = redis.jsonGet(key, jpRoot);
      assertThat(result.toString()).isEqualTo(
            """
                  [[{"name":"Example Object","id":123,"obj1":{"type":"Nested Object","status":"active","nestedObj":{"level":2,"description":"This is a deeply nested object"}}}]]""");

      // Test replace field on non root
      update = """
            {"status": true }
               """;
      jvUpdate = defaultJsonParser.createJsonValue(update);
      assertThat(redis.jsonMerge(key, new JsonPath("$.obj1"), jvUpdate)).isEqualTo("OK");
      result = redis.jsonGet(key, jpRoot);
      assertThat(result.toString()).isEqualTo(
            """
                  [[{"name":"Example Object","id":123,"obj1":{"type":"Nested Object","status":true,"nestedObj":{"level":2,"description":"This is a deeply nested object"}}}]]""");

      // Test create field leaf on non root
      update = """
            {"added":"field"}
               """;
      jvUpdate = defaultJsonParser.createJsonValue(update);
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");
      assertThat(redis.jsonMerge(key, new JsonPath("$.obj1.nonexist"), jvUpdate)).isEqualTo("OK");
      result = redis.jsonGet(key, jpRoot);
      assertThat(result.toString()).isEqualTo(
            """
                  [[{"name":"Example Object","id":123,"obj1":{"type":"Nested Object","status":"active","nestedObj":{"level":2,"description":"This is a deeply nested object"},"nonexist":{"added":"field"}}}]]""");

      // Test create field on non leaf
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");
      assertThat(redis.jsonMerge(key, new JsonPath("$.nonexist1.nonexist2"), jvUpdate)).isNull();


      jv = defaultJsonParser.createJsonValue("""
            {"o11":{"o21":{"o31":"field31", "sameKey": null}, "sameKey": "value", "o22": {"o32": 42}}, "sameKey": {"o22":true }}
               """);
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");
      assertThat(redis.jsonMerge(key, new JsonPath("$..sameKey"), jvUpdate)).isEqualTo("OK");
      result = redis.jsonGet(key, jpRoot);
      assertThat(result.toString()).isEqualTo(
            """
      [[{"o11":{"o21":{"o31":"field31","sameKey":{"added":"field"}},"sameKey":{"added":"field"},"o22":{"o32":42}},"sameKey":{"o22":true,"added":"field"}}]]"""
      );

      // Test create field on non leaf
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");
      var updateFinal = update;
      var jvUpdateFinal = jvUpdate;
      assertThatThrownBy(() -> redis.jsonMerge(key, new JsonPath("$..nonexist1"), jvUpdateFinal)).isInstanceOf(RedisCommandExecutionException.class)
      .hasMessage("ERR Err wrong static path");
   }

   @Test
   public void testJSONARRINSERT() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      JsonPath jp = new JsonPath("$");
      JsonValue jv = defaultJsonParser.createJsonValue("[1, \"a\", {\"o\":\"v\"}]");
      String key = k();

      // Append to root
      redis.jsonSet(key, jp, jv);
      Long res = (Long) command.jsonArrinsert(key, "$", 2, "\"aString\"", "1", "{\"aObj\": null }");
      assertThat(res).isEqualTo(6);
      List<JsonValue> jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[[1,\"a\",\"aString\",1,{\"aObj\":null},{\"o\":\"v\"}]]");

      // Test negative index
      redis.jsonSet(key, jp, jv);
      res = (Long) command.jsonArrinsert(key, "$", -1, "1", "2", "3");
      assertThat(res).isEqualTo(6);
      jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[[1,\"a\",1,2,3,{\"o\":\"v\"}]]");

      // Test out of bounds index
      assertThatThrownBy(() -> command.jsonArrinsert(key, "$", 99, "1", "2", "3"))
            .isInstanceOf(RedisCommandExecutionException.class).hasMessage("ERR index out of bounds");
      jv = defaultJsonParser.createJsonValue("""
               {"a": 2,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"a": true,
                  "nested2": {
                   "foo": "fore"},
                   "arr_value": [1,2,3,4]
                  },
               "nested1":
                  {
                   "arr_value": null
                  },
               "nested2":
                  {
                   "arr_value": 1,
                   "string": "aString"
                  }
               }
            """);
      redis.jsonSet(key, jp, jv);

      // Insert to single element (at the end)
      jp = new JsonPath("$.nested.arr_value");
      JsonValue v1 = defaultJsonParser.fromObject("aString");
      JsonValue v2 = defaultJsonParser.fromObject(1);
      JsonValue v3 = defaultJsonParser.createJsonValue("{\"aObj\": null}");
      List<Long> jsonArrappend = redis.jsonArrinsert(key, jp, 4, v1, v2, v3);
      assertThat(jsonArrappend).containsExactly(7L);
      jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[[1,2,3,4,\"aString\",1,{\"aObj\":null}]]");

      // Append to multiple elements
      jp = new JsonPath("$..arr_value");
      assertThat(redis.jsonArrinsert(key, jp, 1, v1, v2, v3)).containsExactly(6L, 10L, null, null);
      jp = new JsonPath("$.nested.*");
      assertThat(redis.jsonArrinsert(key, jp, 1, v1, v2, v3)).containsExactly(null, null, 13L);
      jp = new JsonPath("$.nested2.*");
      assertThat(redis.jsonArrinsert(key, jp, 1, v1, v2, v3)).containsExactly(null, null);
      jp = new JsonPath("$.non-existent");
      assertThat(redis.jsonArrinsert(key, jp, 1, v1, v2, v3)).isEmpty();

      // Test legacy path
      jp = new JsonPath("$");
      jv = defaultJsonParser.createJsonValue("[1, \"a\", {\"o\":\"v\"}]");
      // Append to root
      redis.jsonSet(key, jp, jv);
      res = (Long) command.jsonArrinsert(key, ".", 2, "\"aString\"", "1", "{\"aObj\": null }");
      assertThat(res).isEqualTo(6);
      jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[[1,\"a\",\"aString\",1,{\"aObj\":null},{\"o\":\"v\"}]]");
      assertThatThrownBy(() -> command.jsonArrinsert("non-existent", "$", 1, "v1", "v2", "v3"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR could not perform this operation on a key that doesn't exist");

      jv = defaultJsonParser.createJsonValue("""
               {"a": 2,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"a": true,
                  "nested2": {
                   "foo": "fore"},
                   "arr_value": [1,2,3,4]
                  },
               "nested1":
                  {
                   "arr_value": null
                  },
               "nested2":
                  {
                   "arr_value": 1
                  }
               }
            """);

      redis.jsonSet(key, jp, jv);
      // Insert to single element (at the end)
      jp = new JsonPath(".nested.arr_value");
      JsonValue v4 = defaultJsonParser.fromObject("aString");
      JsonValue v5 = defaultJsonParser.fromObject(1);
      JsonValue v6 = defaultJsonParser.createJsonValue("{\"aObj\": null}");
      jsonArrappend = redis.jsonArrinsert(key, jp, 4, v4, v5, v6);
      assertThat(jsonArrappend).containsExactly(7L);
      jsonGet = redis.jsonGet(key, jp);
      assertThat(jsonGet.get(0).toString()).isEqualTo("[1,2,3,4,\"aString\",1,{\"aObj\":null}]");
      // Append to multiple elements
      jp = new JsonPath("..arr_value");
      assertThat(redis.jsonArrinsert(key, jp, 1, v4, v5, v6)).containsExactly(10L);
      jp = new JsonPath(".nested.*");
      assertThat(redis.jsonArrinsert(key, jp, 1, v4, v5, v6)).containsExactly(13L);
      JsonPath jp1 = new JsonPath(".nested2.*");
      assertThatThrownBy(() -> redis.jsonArrinsert(key, jp1, 1, v4, v5, v6))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR Path '$.nested2.*' does not exist or not an array");
      JsonPath jp2 = new JsonPath(".non-existent");
      assertThatThrownBy(() -> redis.jsonArrinsert(key, jp2, 1, v4, v5, v6))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR Path '$.non-existent' does not exist or not an array");
      assertThatThrownBy(() -> command.jsonArrinsert("non-existent", "$", 1, "v1", "v2", "v3"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR could not perform this operation on a key that doesn't exist");
   }

   @Test
   public void testJSONARRINSERTAtIndexZero() {
      JsonPath jp = new JsonPath(".");
      String key = k();

      // JSON.SET test . [1]
      redis.jsonSet(key, jp, defaultJsonParser.createJsonValue("[1]"));

      // JSON.ARRINSERT test . 0 -1
      JsonValue valueMinus1 = defaultJsonParser.fromObject(-1);
      List<Long> res2 = redis.jsonArrinsert(key, jp, 0, valueMinus1);
      assertThat(res2).containsExactly(2L);

      // JSON.GET test .
      List<JsonValue> result = redis.jsonGet(key, jp);
      assertThat(result.get(0).toString()).isEqualTo("[-1,1]");
   }

   @Test
   void testJSONMSET() {
      String key1 = k(1);
      String key2 = k(2);
      String key3 = k(3);
      JsonPath jpRoot = new JsonPath("$");
      JsonPath jpRootLegacy = new JsonPath(".");
      JsonValue jv1 = defaultJsonParser.createJsonValue("""
               {"a": 2,
               "arr_value": ["one", "two", "three"],
               "nested":
                  {"a": true,
                  "nested2": {
                   "foo": "fore"},
                   "arr_value": [1,2,3,4]
                  }
               }
            """);
      JsonValue jv2 = defaultJsonParser.createJsonValue("\"string\"");
      JsonValue jv3 = defaultJsonParser.createJsonValue("3");
      List<JsonMsetArgs<String, String>> msetArgs = new ArrayList<>();
      msetArgs.add(new JsonMsetArgs<String, String>(key1, jpRoot, jv1));
      msetArgs.add(new JsonMsetArgs<String, String>(key2, jpRoot, jv2));
      msetArgs.add(new JsonMsetArgs<String, String>(key3, jpRoot, jv3));
      assertThat(redis.jsonMSet(msetArgs)).isEqualTo("OK");
      List<JsonValue> jsonGet = redis.jsonGet(key1, jpRootLegacy);
      assertThat(jsonGet).hasSize(1);
      assertThat(jsonGet.get(0).toString()).isEqualTo(jv1.toString());
      jsonGet = redis.jsonGet(key2, jpRootLegacy);
      assertThat(jsonGet).hasSize(1);
      assertThat(jsonGet.get(0).toString()).isEqualTo(jv2.toString());
      jsonGet = redis.jsonGet(key3, jpRootLegacy);
      assertThat(jsonGet).hasSize(1);
      assertThat(jsonGet.get(0).toString()).isEqualTo(jv3.toString());
      JsonValue jv4 = defaultJsonParser.createJsonValue("{\"k1\":\"v1\"}");
      JsonPath jp = new JsonPath("$.added");
      msetArgs = new ArrayList<>();
      // Adding a leaf to an existing object
      msetArgs.add(new JsonMsetArgs<String, String>(key1, jp, jv4));
      // Replacing a root object
      msetArgs.add(new JsonMsetArgs<String, String>(key2, jpRoot, jv4));
      // Trying to add a leaf to a non object
      msetArgs.add(new JsonMsetArgs<String, String>(key3, jp, jv4));
      assertThat(redis.jsonMSet(msetArgs)).isEqualTo("OK");
      jsonGet = redis.jsonGet(key1, jpRootLegacy);
      assertThat(jsonGet).hasSize(1);
      assertThat(jsonGet.get(0).toString()).isEqualTo(
            """
                  {"a":2,"arr_value":["one","two","three"],"nested":{"a":true,"nested2":{"foo":"fore"},"arr_value":[1,2,3,4]},"added":{"k1":"v1"}}""");
      jsonGet = redis.jsonGet(key2, jpRootLegacy);
      assertThat(jsonGet).hasSize(1);
      assertThat(jsonGet.get(0).toString()).isEqualTo(jv4.toString());
      jsonGet = redis.jsonGet(key3, jpRootLegacy);
      assertThat(jsonGet).hasSize(1);
      assertThat(jsonGet.get(0).toString()).isEqualTo(jv3.toString());

      // Test an error behavior
      List<JsonMsetArgs<String, String>> msetArgs1 = new ArrayList<>();
      msetArgs1.add(new JsonMsetArgs<String, String>("non-existent", jp, jv4));
      msetArgs1.add(new JsonMsetArgs<String, String>(key2, jpRoot, jv4));
      assertThatThrownBy(() -> redis.jsonMSet(msetArgs1)).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR new objects must be created at root");
   }

   public void testJSONMGET() {
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jv1 = defaultJsonParser.createJsonValue("[1, \"a\", {\"o\":\"v\"}]");
      String key1 = k();
      String key2 = k(1);
      String key3 = k(2);
      JsonValue jv2 = defaultJsonParser.createJsonValue("""
            {"a": 2,
            "arr_value": ["one", "two", "three"],
            "nested":
               {"a": true,
               "nested2": {
                "foo": "fore"},
                "arr_value": [1,2,3,4]
               },
               "nested1":
                  {
                   "arr_value": null
                  },
               "nested2":
                  {
                   "arr_value": 1,
                   "string": "aString"
                  }
               }
            """);
      JsonValue jv3 = defaultJsonParser.createJsonValue("\"string\"");
      redis.jsonSet(key1, jpRoot, jv1);
      redis.jsonSet(key2, jpRoot, jv2);
      redis.jsonSet(key3, jpRoot, jv3);
      redis.set("not-a-json", "a-string");
      List<JsonValue> jsonValues = redis.jsonMGet(jpRoot, key1, key2, key3);
      assertThat(jsonValues).map(jv -> jv.asJsonArray().size()).containsExactly(1, 1, 1);
      assertThat(jsonValues).map(jv -> jv.asJsonArray().getFirst().asString()).containsExactly(jv1.asString(),
            jv2.asString(), jv3.asString());
      // Not testing non-existent key, bug in lettuce?
      // see https://github.com/redis/lettuce/issues/3196
      // var result = redis.jsonMGet(jpRoot, key1, "non-existent", key2);
      // var result = redis.jsonMGet(jpRoot, "not-a-json", key1, key2);
   }

   @Test
   public void testJSONMGETSingleKey() {
      // Test for issue #16223: JSON.MGET should accept single key
      String key1 = k();
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jv1 = defaultJsonParser.createJsonValue("""
            {"a":1, "b": 2, "nested": {"a": 3}, "c": null}
            """);
      redis.jsonSet(key1, jpRoot, jv1);

      // Test with single key and JSONPath
      List<JsonValue> result = redis.jsonMGet(new JsonPath("$..a"), key1);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).toString()).isEqualTo("[1,3]");
   }

   public void testJSONMGETNonExistingPath() {
      String key1 = k();
      String key2 = k(1);
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jv1 = defaultJsonParser.createJsonValue("""
            {"a":1, "b": 2, "nested": {"a": 3}, "c": null}
            """);
      JsonValue jv2 = defaultJsonParser.createJsonValue("""
            {"a":4, "b": 5, "nested": {"a": 6}, "c": null}
            """);
      redis.jsonSet(key1, jpRoot, jv1);
      redis.jsonSet(key2, jpRoot, jv2);

      // Test with JSONPath syntax ($.path) - should return empty arrays
      List<JsonValue> result = redis.jsonMGet(new JsonPath("$.42isnotapath"), key1, key2);
      assertThat(result).hasSize(2);
      assertThat(result.get(0).toString()).isEqualTo("[]");
      assertThat(result.get(1).toString()).isEqualTo("[]");

      // Test with legacy path syntax (.path) - should return null for both keys
      result = redis.jsonMGet(new JsonPath(".42isnotapath"), key1, key2);
      assertThat(result).hasSize(2);
      assertThat(result.get(0).isNull()).isTrue();
      assertThat(result.get(1).isNull()).isTrue();
   }

   // Lettuce Json object doesn't implement comparison. Implementing here
   private boolean compareJSONGet(JsonValue result, JsonValue expected, JsonPath... paths) {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode expectedObjectNode, resultNode;
      if (paths.length == 0) {
         paths = new JsonPath[] { new JsonPath("$") };
      }
      try {
         expectedObjectNode = mapper.readTree(expected.toString());
         resultNode = mapper.readTree(result.toString());
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
            JsonNode node = isLegacy ? ((ArrayNode) jpCtx.read(pathStr)).get(0) : jpCtx.read(pathStr);
            return resultNode.equals(node);
         }
         ObjectNode root = mapper.createObjectNode();
         for (JsonPath path : paths) {
            String pathStr = path.toString();
            JsonNode node = isLegacy ? ((ArrayNode) jpCtx.read(pathStr)).get(0) : jpCtx.read(pathStr);
            root.set(pathStr, node);
         }
         return resultNode.equals(root);
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      }
   }

   @Test
   void testJSONRESP() {
      JsonValue jv1 = defaultJsonParser.createJsonValue(
            """
                     {"name":"Wireless earbuds","description":"Wireless Bluetooth in-ear headphones","connection":{"wireless":true,"null":null,"type":"Bluetooth"},"price":64.99,"stock":17,"colors":["black","white"], "max_level":[80, 100, 120]}
                  """);
      JsonPath jpRoot = new JsonPath("$");
      String key = k();
      redis.jsonSet(key, jpRoot, jv1, null);
      List<Object> list = resp(key, "$");
      String s = list.toString();
      assertThat(s).isEqualTo(
            "[[{, name, Wireless earbuds, description, Wireless Bluetooth in-ear headphones, connection, [{, wireless, true, null, null, type, Bluetooth], price, 64.99, stock, 17, colors, [[, black, white], max_level, [[, 80, 100, 120]]]");
      list = resp(key, "$.price");
      assertThat(list).containsExactly(64.99);
      list = resp(key, "$.colors");
      assertThat((List)list.get(0)).containsExactly("[", "black", "white");
      list = resp(key, "$.connection.*");
      assertThat(list).containsExactly(true, null, "Bluetooth");
      assertThat(resp(key, "$.non-existent")).isEmpty();
      assertThatThrownBy(() -> resp(key, ".non-existent")).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR Path '$.non-existent' does not exist");
      assertThat(resp("non-existent", "$")).containsExactly((Object) null);
   }

   @Test
   void testJSONDEBUG() {
      JsonPath jpRoot = new JsonPath("$");
      String key = "doc";
      // JSON.DEBUG MEMORY doc .
      assertThat(jsonDebugLegacy("MEMORY", key, ".")).isZero();
      assertThat(jsonDebug("MEMORY", key, "$")).containsExactly(0L);
      // JSON.SET doc $ '"a"'
      JsonValue jv = defaultJsonParser.createJsonValue("\"a\"");
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");
      // JSON.DEBUG MEMORY doc .
      assertThat(jsonDebugLegacy("MEMORY", key, ".")).isEqualTo(24L);
      assertThat(jsonDebug("MEMORY", key, "$")).containsExactly(24L);
      // JSON.SET doc $ '{"a":"b", "b": [{"a":2}, {"a":"c"}, {"a":5.4}, {"a":true},{"h":1}, {"a":["hello"]}]}'
      jv = defaultJsonParser.createJsonValue("""
              {"a":"b", "b": [{"a":2}, {"a":"c"}, {"a":5.4}, {"a":true}, {"a":["hello"]}]}
            """);
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");
      // JSON.DEBUG MEMORY doc .
      assertThat(jsonDebugLegacy("MEMORY", key, ".")).isEqualTo(88L);
      assertThat(jsonDebug("MEMORY", key, "$")).containsExactly(88L);
      assertThatThrownBy(() -> jsonDebug("BADCOMMAND", key, "$")).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR unknown subcommand - try `JSON.DEBUG HELP`");
   }

   private Long jsonDebugLegacy(String subCommand, String key, String path) {
      RedisCodec<String, String> codec = StringCodec.UTF8;
      return redis.dispatch(TestCommandType.JSON_DEBUG, new IntegerOutput<>(codec),
              new CommandArgs<>(codec).add(subCommand).addKey(key).add(path));
   }

   private List<Long> jsonDebug(String subCommand, String key, String path) {
      RedisCodec<String, String> codec = StringCodec.UTF8;
      return redis.dispatch(TestCommandType.JSON_DEBUG, new IntegerListOutput<>(codec),
              new CommandArgs<>(codec).add(subCommand).addKey(key).add(path));
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

   // Using dispatch to allow boolean in command output
   private List<Object> resp(String key, String path) {
      return redis.dispatch(new ProtocolKeyword() {
         @Override
         public byte[] getBytes() {
            return "JSON.RESP".getBytes(StandardCharsets.UTF_8);
         }
      }, new CustomArrayOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8).addKey(key).addValue(path));
   }

   // Redis ArrayOutput doesn't work with boolean element. Adding a patch for testing
   private static class CustomArrayOutput<K, V> extends ArrayOutput<K, V> {

      public CustomArrayOutput(RedisCodec<K, V> codec) {
         super(codec);
      }

      @Override
      public void set(boolean value) {
         if (output==null) {
            output = new ArrayList<>();
        }

        output.add(value);
      }
   }

}
