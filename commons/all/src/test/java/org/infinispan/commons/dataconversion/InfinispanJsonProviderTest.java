package org.infinispan.commons.dataconversion;

import static com.jayway.jsonpath.JsonPath.using;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.infinispan.commons.dataconversion.internal.InfinispanJsonProvider;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonInfinispanMapperProvider;
import org.junit.Test;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.internal.Path;
import com.jayway.jsonpath.internal.path.PredicateContextImpl;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.mapper.JsonSmartMappingProvider;

public class InfinispanJsonProviderTest extends JsonProviderBaseTest {
   private static final String JSON = "[" +
         "{\n" +
         "   \"foo\" : \"foo0\",\n" +
         "   \"bar\" : 0,\n" +
         "   \"baz\" : true,\n" +
         "   \"gen\" : {\"eric\" : \"yepp\"}" +
         "}," +
         "{\n" +
         "   \"foo\" : \"foo1\",\n" +
         "   \"bar\" : 1,\n" +
         "   \"baz\" : true,\n" +
         "   \"gen\" : {\"eric\" : \"yepp\"}" +
         "}," +
         "{\n" +
         "   \"foo\" : \"foo2\",\n" +
         "   \"bar\" : 2,\n" +
         "   \"baz\" : true,\n" +
         "   \"gen\" : {\"eric\" : \"yepp\"}" +
         "}" +
         "]";

   @Test
   public void json_can_be_parsed() {
      Json node = using(INFINISPAN_ORG_CONFIGURATION).parse(JSON_DOCUMENT).read("$");
      Json at = node.at("string-property");
      assertEquals(at.asString(), "string-value");
   }

   @Test
   public void strings_are_unwrapped() {
       DocumentContext documentContext = using(INFINISPAN_ORG_CONFIGURATION).parse(JSON_DOCUMENT);
      assertEquals("string-value", documentContext.read("$.string-property", String.class));
   }


   @Test
   public void integers_are_unwrapped() {
       var actual = using(INFINISPAN_ORG_CONFIGURATION).parse(JSON_DOCUMENT).read("$.int-max-property", Integer.class);
      assertEquals((Integer)Integer.MAX_VALUE, actual);
   }


   @Test
   public void ints_are_unwrapped() {
       assertEquals(Integer.MAX_VALUE,(int)using(INFINISPAN_ORG_CONFIGURATION).parse(JSON_DOCUMENT).read("$.int-max-property", int.class));
   }

   @Test
   public void longs_are_unwrapped() {
         Json node = using(INFINISPAN_ORG_CONFIGURATION).parse(JSON_DOCUMENT).read("$.long-max-property");
      long val = using(INFINISPAN_ORG_CONFIGURATION).parse(JSON_DOCUMENT).read("$.long-max-property", Long.class);
      assertEquals(Long.MAX_VALUE, val);
      assertEquals(node.asLong(), val);
   }

   @Test
   public void doubles_are_unwrapped() {
      final String json = "{\"double-property\" : 56.78}";
      Json node = using(INFINISPAN_ORG_CONFIGURATION).parse(json).read("$.double-property");
      Double val = using(INFINISPAN_ORG_CONFIGURATION).parse(json).read("$.double-property", Double.class);
      assertEquals(56.78d, val, 0.01);
      assertEquals(node.asDouble(), val, 0.01);
   }

   @Test
   public void int_to_long_mapping() {
      Long actual = using(INFINISPAN_ORG_CONFIGURATION).parse("{\"val\": 1}").read("val", Long.class);
      assertEquals((Long) 1L, actual);
   }

   @Test
   public void an_Integer_can_be_converted_to_a_Double() {
      DocumentContext documentContext = using(INFINISPAN_ORG_CONFIGURATION).parse("{\"val\": 1}");
      assertEquals((Double) 1D, documentContext.read("val", Double.class));
   }

   @Test
   public void list_of_numbers() {
      DocumentContext documentContext = using(INFINISPAN_ORG_CONFIGURATION).parse(JSON_DOCUMENT);
      ArrayList<Json> objs = documentContext.read("$.store.book[*].display-price");
      List<Double> actual = new ArrayList<>();
      for (Json obj : objs) {
         actual.add(obj.asDouble());
      }
      assertArrayEquals(actual.toArray(new Double[0]), new Double[] { 8.95D, 12.99D, 8.99D, 22.99D });
   }

   @SuppressWarnings("unchecked")
   @Test
   public void list_of_numbers_as_list() {
      DocumentContext documentContext = using(INFINISPAN_ORG_CONFIGURATION).parse(JSON_DOCUMENT);
      List<Json> oList = documentContext.read("$.store.book[*].display-price", ArrayList.class);
      List<Double> dList = oList.stream().map(Json::asDouble).toList();
      assertArrayEquals(dList.toArray(new Double[0]), new Double[] { 8.95D, 12.99D, 8.99D, 22.99D });

      // Let's see if our ouput is good for input
      String s = documentContext.read("$.store.book", String.class);
      DocumentContext booksContext = using(INFINISPAN_ORG_CONFIGURATION).parse(s);
      List<Json> oList2 = booksContext.read("$[*].display-price", ArrayList.class);
      List<Double> dList2 = oList2.stream().map(Json::asDouble).toList();
      assertArrayEquals(dList2.toArray(new Double[0]), new Double[] { 8.95D, 12.99D, 8.99D, 22.99D });
      List<Json> oList3 = booksContext.read("$[*].category", ArrayList.class);
      List<String> sList3 = oList3.stream().map(Json::asString).toList();
      // assertArrayEquals(actua l1.toArray(new Double[0]), new Double[]{8.95D, 12.99D,
      // 8.99D, 22.99D});
      assertArrayEquals(sList3.toArray(new String[0]), new String[] { "reference", "fiction", "classic", "fiction" });
   }

    @Test
    public void an_root_property_can_be_updated() {
        DocumentContext documentContext = using(INFINISPAN_ORG_CONFIGURATION).parse(JSON_DOCUMENT);
        documentContext.set("$.int-max-property", 1);
        Integer result = documentContext.read("$.int-max-property", Integer.class);
        assertEquals((Integer)1, result);
    }

   @Test
   public void write_array() {
      DocumentContext documentContext = using(INFINISPAN_ORG_CONFIGURATION).parse("{\"val\": [1,2,3,4]}");
      documentContext.set("val", Arrays.asList(10, 20, 30));
      Json read = documentContext.read("val");
      assertTrue(read.isArray());
      List<Double> actual = new ArrayList<>();
      for (Json obj : read.asJsonList()) {
         actual.add(obj.asDouble());
      }
      assertArrayEquals(actual.toArray(new Double[0]), new Double[] { 10D, 20D, 30D });
   }

   // @Test
   // public void an_object_can_be_mapped_to_pojo() {

   // String json = "{\n" +
   // " \"foo\" : \"foo\",\n" +
   // " \"bar\" : 10,\n" +
   // " \"baz\" : true\n" +
   // "}";

   // TestClazz testClazz =
   // JsonPath.using(GSON_CONFIGURATION).parse(json).read("$", TestClazz.class);

   // assertThat(testClazz.foo).isEqualTo("foo");
   // assertThat(testClazz.bar).isEqualTo(10L);
   // assertThat(testClazz.baz).isEqualTo(true);

   // }

   // @Test
   // public void test_type_ref() throws IOException {
   // TypeRef<List<FooBarBaz<Gen>>> typeRef = new TypeRef<List<FooBarBaz<Gen>>>() {
   // };

   // List<FooBarBaz<Gen>> list =
   // JsonPath.using(GSON_CONFIGURATION).parse(JSON).read("$", typeRef);

   // assertThat(list.get(0).gen.eric).isEqualTo("yepp");
   // }

   // @Test
   // public void test_type_ref_fail() throws IOException {
   // TypeRef<List<FooBarBaz<Integer>>> typeRef = new
   // TypeRef<List<FooBarBaz<Integer>>>() {
   // };

   // assertThrows(MappingException.class, () ->
   // using(GSON_CONFIGURATION).parse(JSON).read("$", typeRef));
   // }

   // @Test
   // // https://github.com/json-path/JsonPath/issues/351
   // public void no_error_when_mapping_null() throws IOException {

   // Configuration configuration = Configuration
   // .builder()
   // .mappingProvider(new GsonMappingProvider())
   // .jsonProvider(new GsonJsonProvider())
   // .options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS)
   // .build();

   // String json = "{\"M\":[]}";

   // String result = JsonPath.using(configuration).parse(json).read("$.M[0].A[0]",
   // String.class);

   // assertThat(result).isNull();
   // }

   public static class FooBarBaz<T> {
      public T gen;
      public String foo;
      public Long bar;
      public boolean baz;
   }

   public static class Gen {
      public String eric;
   }

   public static class TestClazz {
      public String foo;
      public Long bar;
      public boolean baz;
   }
}

class JsonProviderBaseTest {
   public static final Configuration INFINISPAN_ORG_CONFIGURATION = Configuration
         .builder()
         .mappingProvider(new JsonInfinispanMapperProvider())
         .jsonProvider(new InfinispanJsonProvider())
         .build();

   // public static final Configuration JSON_ORG_CONFIGURATION = Configuration
   // .builder()
   // .mappingProvider(new JsonOrgMappingProvider())
   // .jsonProvider(new JsonOrgJsonProvider())
   // .build();

   // public static final Configuration GSON_CONFIGURATION = Configuration
   // .builder()
   // .mappingProvider(new GsonMappingProvider())
   // .jsonProvider(new GsonJsonProvider())
   // .build();

   // public static final Configuration JACKSON_CONFIGURATION = Configuration
   // .builder()
   // .mappingProvider(new JacksonMappingProvider())
   // .jsonProvider(new JacksonJsonProvider())
   // .build();

   // public static final Configuration JACKSON_JSON_NODE_CONFIGURATION =
   // Configuration
   // .builder()
   // .mappingProvider(new JacksonMappingProvider())
   // .jsonProvider(new JacksonJsonNodeJsonProvider())
   // .build();

   // public static final Configuration JETTISON_CONFIGURATION = Configuration
   // .builder()
   // .jsonProvider(new JettisonProvider())
   // .build();

   public static final Configuration JSON_SMART_CONFIGURATION = Configuration
         .builder()
         .mappingProvider(new JsonSmartMappingProvider())
         .jsonProvider(new JsonSmartJsonProvider())
         .build();

   // public static final Configuration TAPESTRY_JSON_CONFIGURATION = Configuration
   // .builder()
   // .mappingProvider(new TapestryMappingProvider())
   // .jsonProvider(TapestryJsonProvider.INSTANCE)
   // .build();

   // public static final Configuration JAKARTA_JSON_CONFIGURATION = Configuration
   // .builder()
   // .mappingProvider(new JakartaMappingProvider())
   // .jsonProvider(new JakartaJsonProvider())
   // .build();

   // // extension to Jakarta EE 9 JSON-P with mutable objects and array
   // public static final Configuration JAKARTA_JSON_RW_CONFIGURATION =
   // Configuration
   // .builder()
   // .mappingProvider(new JakartaMappingProvider())
   // .jsonProvider(new JakartaJsonProvider(true))
   // .build();

   public static final String JSON_BOOK_DOCUMENT = "{ " +
         "   \"category\" : \"reference\",\n" +
         "   \"author\" : \"Nigel Rees\",\n" +
         "   \"title\" : \"Sayings of the Century\",\n" +
         "   \"display-price\" : 8.95\n" +
         "}";
   public static final String JSON_DOCUMENT = "{\n" +
         "   \"string-property\" : \"string-value\", \n" +
         "   \"int-max-property\" : " + Integer.MAX_VALUE + ", \n" +
         "   \"long-max-property\" : " + Long.MAX_VALUE + ", \n" +
         "   \"boolean-property\" : true, \n" +
         "   \"null-property\" : null, \n" +
         "   \"int-small-property\" : 1, \n" +
         "   \"max-price\" : 10, \n" +
         "   \"store\" : {\n" +
         "      \"book\" : [\n" +
         "         {\n" +
         "            \"category\" : \"reference\",\n" +
         "            \"author\" : \"Nigel Rees\",\n" +
         "            \"title\" : \"Sayings of the Century\",\n" +
         "            \"display-price\" : 8.95\n" +
         "         },\n" +
         "         {\n" +
         "            \"category\" : \"fiction\",\n" +
         "            \"author\" : \"Evelyn Waugh\",\n" +
         "            \"title\" : \"Sword of Honour\",\n" +
         "            \"display-price\" : 12.99\n" +
         "         },\n" +
         "         {\n" +
         "            \"category\" : \"classic\",\n" +
         "            \"author\" : \"Herman Melville\",\n" +
         "            \"title\" : \"Moby Dick\",\n" +
         "            \"isbn\" : \"0-553-21311-3\",\n" +
         "            \"display-price\" : 8.99\n" +
         "         },\n" +
         "         {\n" +
         "            \"category\" : \"fiction\",\n" +
         "            \"author\" : \"J. R. R. Tolkien\",\n" +
         "            \"title\" : \"The Lord of the Rings\",\n" +
         "            \"isbn\" : \"0-395-19395-8\",\n" +
         "            \"display-price\" : 22.99\n" +
         "         }\n" +
         "      ],\n" +
         "      \"bicycle\" : {\n" +
         "         \"foo\" : \"baz\",\n" +
         "         \"escape\" : \"Esc\\b\\f\\n\\r\\t\\n\\t\\u002A\",\n" +
         "         \"color\" : \"red\",\n" +
         "         \"display-price\" : 19.95,\n" +
         "         \"foo:bar\" : \"fooBar\",\n" +
         "         \"dot.notation\" : \"new\",\n" +
         "         \"dash-notation\" : \"dashes\"\n" +
         "      }\n" +
         "   },\n" +
         "   \"foo\" : \"bar\",\n" +
         "   \"@id\" : \"ID\"\n" +
         "}";

   public static String JSON_BOOK_STORE_DOCUMENT = "{\n" +
         "    \"store\": {\n" +
         "        \"book\": [\n" +
         "            {\n" +
         "                \"category\": \"reference\"\n" +
         "            },\n" +
         "            {\n" +
         "                \"category\": \"fiction\"\n" +
         "            },\n" +
         "            {\n" +
         "                \"category\": \"fiction\"\n" +
         "            },\n" +
         "            {\n" +
         "                \"category\": \"fiction\"\n" +
         "            }\n" +
         "        ]\n" +
         "    },\n" +
         "    \"expensive\": 10\n" +
         "}";

   public Predicate.PredicateContext createPredicateContext(final Object check) {

      return new PredicateContextImpl(check, check, Configuration.defaultConfiguration(), new HashMap<Path, Object>());
   }
}
