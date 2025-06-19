package org.infinispan.rest.framework.openapi;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.server.core.query.json.JsonQueryRequest;
import org.infinispan.server.core.query.json.JsonQueryResponse;
import org.testng.annotations.Test;

@Test(testName = "rest.framework.openapi.SchemaTest", groups = "unit")
public class SchemaTest {

   public void testStringArray() {
      Schema schema = new Schema(String[].class);
      assertEquals("""
            {"type":"array","items":{"type":"string"}}""", schema.toJson().toString());
   }

   public void testJsonQueryRequest() {
      Schema schema = new Schema(JsonQueryRequest.class);
      assertEquals("", schema.toJson().toString());
   }

   public void testJsonQueryResponse() {
      Schema schema = new Schema(JsonQueryResponse.class);
      assertEquals("", schema.toJson().toString());
   }

}
