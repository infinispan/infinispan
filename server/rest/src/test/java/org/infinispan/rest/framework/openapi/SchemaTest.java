package org.infinispan.rest.framework.openapi;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.rest.distribution.CompleteKeyDistribution;
import org.infinispan.rest.search.entity.Gender;
import org.testng.annotations.Test;

@Test(testName = "rest.framework.openapi.SchemaTest", groups = "unit")
public class SchemaTest {

   public void testStringArray() {
      assertEquals("""
            {"type":"array","items":{"type":"string"}}""", new Schema(String[].class).toJson().toString());
   }

   public void testCompleteKeyDistributionArray() {
      assertEquals("""
            {"type":"array","items":{"type":"string"}}""", new Schema(CompleteKeyDistribution[].class).toJson().toString());
   }

   public void testEnum() {
      assertEquals("""
            {"type":"string","enum":["MALE","FEMALE"]}""", new Schema(Gender.class).toJson().toString());
   }

}
