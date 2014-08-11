package org.infinispan.server.websocket.json;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.infinispan.assertions.JsonPayloadAssertion.assertThat;
import static org.testng.Assert.assertEquals;

/**
 * Tests JsonObject class.
 *
 * @author Sebastian Laskawiec
 */
@Test(testName = "websocket.json.JsonObjectTest", groups = "unit")
public class JsonObjectTest {

   private static class ExampleObject {

      String field1;
      String field2;

      ExampleObject(String field1, String field2) {
         this.field1 = field1;
         this.field2 = field2;
      }

      public String getField1() {
         return field1;
      }

      public void setField1(String field1) {
         this.field1 = field1;
      }

      public String getField2() {
         return field2;
      }

      public void setField2(String field2) {
         this.field2 = field2;
      }
   }

   public void shouldReadJsonObjectFromJavaObject() throws Exception {
      //given
      ExampleObject example = new ExampleObject("field1Val", "field2Val");

      //when
      JsonObject testedObject = JsonObject.fromObject(example);

      //then
      assertThat(testedObject).hasFields("field1", "field1Val").hasFields("field2", "field2Val");
   }

   public void shouldReadJsonObjectFromMap() throws Exception {
      //given
      Map<Object, Object> objectMap = new HashMap<>();
      objectMap.put("field1", "field1Val");
      objectMap.put("field2", "field2Val");

      //when
      JsonObject testedObject = JsonObject.fromObject(objectMap);

      //then
      assertThat(testedObject).hasFields("field1", "field1Val").hasFields("field2", "field2Val");
   }

   public void shouldOverrideToStringMethod() throws Exception {
      //given
      Map<String, Object> objectMap = new LinkedHashMap<>();
      objectMap.put("field2", "field2Val");
      objectMap.put("field1", "field1Val");
      JsonObject testedObject = JsonObject.fromMap(objectMap);

      //when
      String json = testedObject.toString();

      //then
      assertEquals("{\"field2\":\"field2Val\",\"field1\":\"field1Val\"}", json);
   }

}