package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.k;

import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.json.DefaultJsonParser;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.json.JsonValue;

@Test(groups = "functional", testName = "server.resp.JsonFilterCommandsTest")
public class JsonFilterCommandsTest extends SingleNodeRespBaseTest {
   /**
    * RESP JSON filter commands testing
    *
    * @since 15.2
    */
   private RedisCommands<String, String> redis;
   private final DefaultJsonParser defaultJsonParser = new DefaultJsonParser();

   @Override
   public Object[] factory() {
      return new Object[] {
         new JsonFilterCommandsTest(),
         new JsonFilterCommandsTest().withAuthorization()
      };
   }

   @BeforeMethod
   public void initConnection() {
      redis = redisConnection.sync();
   }

   @Test
   public void testJSONGETWithFilterExpression() {
      // Test JSONPath filter expressions with regex matching
      JsonPath jpRoot = new JsonPath("$");
      String key = k();

      // JSON.SET doc $ '{"arr":["foo","bar","foobar","bazfoo","baz"]}'
      JsonValue jv = defaultJsonParser.createJsonValue("""
            {"arr":["foo","bar","foobar","bazfoo","baz"]}
            """);
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");

      // JSON.GET doc '$.arr[?(@ =~ ".*foo")]'
      // Should return elements matching the regex pattern ".*foo"
      JsonPath filterPath = new JsonPath("$.arr[?(@ =~ \".*foo\")]");
      List<JsonValue> result = redis.jsonGet(key, filterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(3);
      assertThat(result.get(0).asJsonArray().asList())
            .map(JsonValue::asString)
            .containsExactly("foo", "foobar", "bazfoo");

      // Same test with Java regex syntax
      JsonPath javaFilterPath = new JsonPath("$.arr[?(@ =~ /.*foo.*/)]");
      result = redis.jsonGet(key, javaFilterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(3);
      assertThat(result.get(0).asJsonArray().asList())
            .map(JsonValue::asString)
            .containsExactly("foo", "foobar", "bazfoo");

      // Test with different filter: exact match
      // JSON.GET doc $.arr[?(@ == "bar")]
      JsonPath exactMatchPath = new JsonPath("$.arr[?(@ == \"bar\")]");
      result = redis.jsonGet(key, exactMatchPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(1);
      assertThat(result.get(0).asJsonArray().getFirst().asString()).isEqualTo("bar");

      // Test with numeric array and comparison filter
      // JSON.SET doc2 $ '{"nums":[1,5,10,15,20,25]}'
      String key2 = k(2);
      JsonValue jv2 = defaultJsonParser.createJsonValue("""
            {"nums":[1,5,10,15,20,25]}
            """);
      assertThat(redis.jsonSet(key2, jpRoot, jv2)).isEqualTo("OK");

      // JSON.GET doc2 $.nums[?(@ > 10)]
      JsonPath numFilterPath = new JsonPath("$.nums[?(@ > 10)]");
      result = redis.jsonGet(key2, numFilterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(3);
      assertThat(result.get(0).asJsonArray().asList())
            .map(JsonValue::asNumber)
            .map(Number::intValue)
            .containsExactly(15, 20, 25);

      // Test with object array and nested property filter
      // JSON.SET doc3 $ '{"items":[{"name":"foo","price":10},{"name":"bar","price":20},{"name":"foobar","price":15}]}'
      String key3 = k(3);
      JsonValue jv3 = defaultJsonParser.createJsonValue("""
            {"items":[{"name":"foo","price":10},{"name":"bar","price":20},{"name":"foobar","price":15}]}
            """);
      assertThat(redis.jsonSet(key3, jpRoot, jv3)).isEqualTo("OK");

      // JSON.GET doc3 $.items[?(@.price > 10)]
      JsonPath objFilterPath = new JsonPath("$.items[?(@.price > 10)]");
      result = redis.jsonGet(key3, objFilterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(2);
      // Should return the two items with price > 10
      String resultStr = result.get(0).toString();
      assertThat(resultStr).isEqualTo("[{\"name\":\"bar\",\"price\":20},{\"name\":\"foobar\",\"price\":15}]");

      // Test with complex array and regex filter
      // JSON.SET doc4 $ '{"arr": ["kaboom", "kafoosh", "four", "bar", 7.0, "foolish", ["FoO", "fight"]]}'
      String key4 = k(4);
      JsonValue jv4 = defaultJsonParser.createJsonValue("""
            {"arr": ["kaboom", "kafoosh", "four", "bar", 7.0, "foolish", ["FoO", "fight"]]}
            """);
      assertThat(redis.jsonSet(key4, jpRoot, jv4)).isEqualTo("OK");

      // JSON.GET doc4 '$.arr[?(@ =~ "foo")]' - should match "kafoosh", "foolish" (case-sensitive partial match)
      JsonPath fooFilterPath = new JsonPath("$.arr[?(@ =~ \"foo\")]");
      result = redis.jsonGet(key4, fooFilterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(2);
      assertThat(result.get(0).asJsonArray().asList())
            .map(JsonValue::asString)
            .containsExactly("kafoosh", "foolish");

      // JSON.GET doc4 '$.arr[?(@ =~ "^ka")]' - should match "kaboom", "kafoosh" (starts with "ka")
      JsonPath kaFilterPath = new JsonPath("$.arr[?(@ =~ \"^ka\")]");
      result = redis.jsonGet(key4, kaFilterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(2);
      assertThat(result.get(0).asJsonArray().asList())
            .map(JsonValue::asString)
            .containsExactly("kaboom", "kafoosh");
   }

   @Test
   public void testJSONGETWithDynamicFilter() {
      // Test JSONPath filter expressions that compare document fields
      JsonPath jpRoot = new JsonPath("$");
      String key = k();

      // Test comparing two numeric fields in the same object
      // JSON.SET doc $ '{"products":[{"name":"laptop","price":1000,"discount":100},{"name":"mouse","price":50,"discount":60},{"name":"keyboard","price":80,"discount":20}]}'
      JsonValue jv = defaultJsonParser.createJsonValue("""
            {"products":[
               {"name":"laptop","price":1000,"discount":100},
               {"name":"mouse","price":50,"discount":60},
               {"name":"keyboard","price":80,"discount":20}
            ]}
            """);
      assertThat(redis.jsonSet(key, jpRoot, jv)).isEqualTo("OK");

      // JSON.GET doc '$.products[?(@.price > @.discount)]' - should return items where price > discount
      JsonPath filterPath = new JsonPath("$.products[?(@.price > @.discount)]");
      List<JsonValue> result = redis.jsonGet(key, filterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(2);
      // Should return laptop and keyboard, but not mouse (50 < 60)
      String resultStr = result.get(0).toString();
      assertThat(resultStr).contains("laptop");
      assertThat(resultStr).contains("keyboard");
      assertThat(resultStr).doesNotContain("mouse");

      // Test with equality comparison
      // JSON.SET doc2 $ '{"users":[{"name":"alice","age":30,"minAge":25},{"name":"bob","age":20,"minAge":20},{"name":"charlie","age":35,"minAge":40}]}'
      String key2 = k(2);
      JsonValue jv2 = defaultJsonParser.createJsonValue("""
            {"users":[
               {"name":"alice","age":30,"minAge":25},
               {"name":"bob","age":20,"minAge":20},
               {"name":"charlie","age":35,"minAge":40}
            ]}
            """);
      assertThat(redis.jsonSet(key2, jpRoot, jv2)).isEqualTo("OK");

      // JSON.GET doc2 '$.users[?(@.age >= @.minAge)]' - should return users where age >= minAge
      JsonPath ageFilterPath = new JsonPath("$.users[?(@.age >= @.minAge)]");
      result = redis.jsonGet(key2, ageFilterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(2);
      resultStr = result.get(0).toString();
      assertThat(resultStr).contains("alice");
      assertThat(resultStr).contains("bob");
      assertThat(resultStr).doesNotContain("charlie");

      // Test with string field comparison
      // JSON.SET doc3 $ '{"items":[{"id":"A","category":"electronics","targetCategory":"electronics"},{"id":"B","category":"books","targetCategory":"electronics"},{"id":"C","category":"toys","targetCategory":"toys"}]}'
      String key3 = k(3);
      JsonValue jv3 = defaultJsonParser.createJsonValue("""
            {"items":[
               {"id":"A","category":"electronics","targetCategory":"electronics"},
               {"id":"B","category":"books","targetCategory":"electronics"},
               {"id":"C","category":"toys","targetCategory":"toys"}
            ]}
            """);
      assertThat(redis.jsonSet(key3, jpRoot, jv3)).isEqualTo("OK");

      // JSON.GET doc3 '$.items[?(@.category == @.targetCategory)]' - should return items where category matches targetCategory
      JsonPath categoryFilterPath = new JsonPath("$.items[?(@.category == @.targetCategory)]");
      result = redis.jsonGet(key3, categoryFilterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(2);
      resultStr = result.get(0).toString();
      assertThat(resultStr).contains("\"id\":\"A\"");
      assertThat(resultStr).contains("\"id\":\"C\"");
      assertThat(resultStr).doesNotContain("\"id\":\"B\"");

      // Test with complex comparison combining field comparison and constant
      // JSON.SET doc4 $ '{"scores":[{"player":"alice","score":100,"threshold":80,"bonus":10},{"player":"bob","score":70,"threshold":80,"bonus":5},{"player":"charlie","score":90,"threshold":85,"bonus":15}]}'
      String key4 = k(4);
      JsonValue jv4 = defaultJsonParser.createJsonValue("""
            {"scores":[
               {"player":"alice","score":100,"threshold":80,"bonus":10},
               {"player":"bob","score":70,"threshold":80,"bonus":5},
               {"player":"charlie","score":90,"threshold":85,"bonus":15}
            ],
            "pattern": "bob"}
            """);
      assertThat(redis.jsonSet(key4, jpRoot, jv4)).isEqualTo("OK");

      // JSON.GET doc4 '$.scores[?(@.score > @.threshold && @.bonus > 10)]' - combined filter
      JsonPath combinedFilterPath = new JsonPath("$.scores[?(@.score > @.threshold && @.bonus > 10)]");
      result = redis.jsonGet(key4, combinedFilterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(1);
      resultStr = result.get(0).toString();
      assertThat(resultStr).contains("charlie");
      assertThat(resultStr).doesNotContain("alice");
      assertThat(resultStr).doesNotContain("bob");

      // Test with pattern not in the current node
      JsonPath dynFilterPath = new JsonPath("$.scores[?(@.player == $.pattern)]");
      result = redis.jsonGet(key4, dynFilterPath);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asJsonArray().size()).isEqualTo(1);
   }
}
