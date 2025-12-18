package org.infinispan.rest.framework.openapi;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.server.core.query.json.JsonQueryRequest;
import org.infinispan.server.core.query.json.JsonQueryResponse;
import org.testng.annotations.Test;

@Test(testName = "rest.framework.openapi.SchemaTest", groups = "unit")
public class SchemaTest {

   // Primitive types tests
   public void testBoolean() {
      Schema schema = new Schema(boolean.class);
      assertEquals("""
            {"type":"boolean"}""", schema.toJson().toString());
   }

   public void testBooleanWrapper() {
      Schema schema = new Schema(Boolean.class);
      assertEquals("""
            {"type":"boolean"}""", schema.toJson().toString());
   }

   public void testInt() {
      Schema schema = new Schema(int.class);
      assertEquals("""
            {"type":"integer","format":"int32"}""", schema.toJson().toString());
   }

   public void testInteger() {
      Schema schema = new Schema(Integer.class);
      assertEquals("""
            {"type":"integer","format":"int32"}""", schema.toJson().toString());
   }

   public void testLong() {
      Schema schema = new Schema(long.class);
      assertEquals("""
            {"type":"integer","format":"int64"}""", schema.toJson().toString());
   }

   public void testLongWrapper() {
      Schema schema = new Schema(Long.class);
      assertEquals("""
            {"type":"integer","format":"int64"}""", schema.toJson().toString());
   }

   public void testFloat() {
      Schema schema = new Schema(float.class);
      assertEquals("""
            {"type":"number","format":"float"}""", schema.toJson().toString());
   }

   public void testDouble() {
      Schema schema = new Schema(double.class);
      assertEquals("""
            {"type":"number","format":"float"}""", schema.toJson().toString());
   }

   public void testShort() {
      Schema schema = new Schema(short.class);
      assertEquals("""
            {"type":"number","minimum":-32768,"maximum":32767}""", schema.toJson().toString());
   }

   public void testShortWrapper() {
      Schema schema = new Schema(Short.class);
      assertEquals("""
            {"type":"number","minimum":-32768,"maximum":32767}""", schema.toJson().toString());
   }

   public void testByte() {
      Schema schema = new Schema(byte.class);
      assertEquals("""
            {"type":"number","minimum":-128,"maximum":127}""", schema.toJson().toString());
   }

   public void testByteWrapper() {
      Schema schema = new Schema(Byte.class);
      assertEquals("""
            {"type":"number","minimum":-128,"maximum":127}""", schema.toJson().toString());
   }

   public void testString() {
      Schema schema = new Schema(String.class);
      assertEquals("""
            {"type":"string"}""", schema.toJson().toString());
   }

   // Array tests
   public void testStringArray() {
      Schema schema = new Schema(String[].class);
      assertEquals("""
            {"type":"array","items":{"type":"string"}}""", schema.toJson().toString());
   }

   public void testIntArray() {
      Schema schema = new Schema(int[].class);
      assertEquals("""
            {"type":"array","items":{"type":"integer","format":"int32"}}""", schema.toJson().toString());
   }

   // Enum test
   public void testEnum() {
      Schema schema = new Schema(TestEnum.class);
      assertEquals("""
            {"type":"string","enum":["VALUE1","VALUE2","VALUE3"]}""", schema.toJson().toString());
   }

   // Object test
   public void testSimpleObject() {
      Schema schema = new Schema(TestObject.class);
      assertEquals("""
            {"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer","format":"int32"},"active":{"type":"boolean"}}}""",
            schema.toJson().toString());
   }

   public void testNestedObject() {
      Schema schema = new Schema(NestedObject.class);
      assertEquals("""
            {"type":"object","properties":{"id":{"type":"integer","format":"int64"},"inner":{"type":"object","properties":{"value":{"type":"string"}}}}}""",
            schema.toJson().toString());
   }

   public void testJsonQueryRequest() {
      Schema schema = new Schema(JsonQueryRequest.class);
      assertEquals("""
            {"type":"object","properties":{"query":{"type":"string"},"start_offset":{"type":"integer","format":"int32"},"max_results":{"type":"integer","format":"int32"},"hit_count_accuracy":{"type":"integer","format":"int32"}}}""",
            schema.toJson().toString());
   }

   public void testJsonQueryResponse() {
      Schema schema = new Schema(JsonQueryResponse.class);
      assertEquals("""
            {"type":"object","properties":{"hit_count":{"type":"integer","format":"int32"},"hit_count_exact":{"type":"boolean"}}}""",
            schema.toJson().toString());
   }

   // Test helper classes
   enum TestEnum {
      VALUE1, VALUE2, VALUE3
   }

   static class TestObject {
      String name;
      int age;
      boolean active;
   }

   static class NestedObject {
      long id;
      InnerObject inner;
   }

   static class InnerObject {
      String value;
   }

}
