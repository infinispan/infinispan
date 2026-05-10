package org.infinispan.rest.framework.impl;

import static org.infinispan.rest.framework.impl.PathInterpreter.resolveVariables;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "rest.framework.PathInterpreterTest")
public class PathInterpreterTest {

   @Test
   public void testSingleVariable() {
      Map<String, String> res = resolveVariables("{id}", "5435");

      assertEquals(1, res.size());
      assertEquals("5435", res.get("id"));
   }

   @Test
   public void testSingleVariable2() {
      Map<String, String> res = resolveVariables("{variable_name}", "a");

      assertEquals(1, res.size());
      assertEquals("a", res.get("variable_name"));
   }

   @Test
   public void testDualVariables() {
      Map<String, String> res = resolveVariables("{cachemanager}-{cache}", "default-mycache");

      assertEquals(2, res.size());
      assertEquals("default", res.get("cachemanager"));
      assertEquals("mycache", res.get("cache"));
   }

   @Test
   public void testDualVariables2() {
      Map<String, String> res = resolveVariables("{cachemanager}{cache}", "defaultmycache");

      assertEquals(0, res.size());
   }

   @Test
   public void testDualVariables3() {
      Map<String, String> res = resolveVariables("{a}:{b}", "value1:value2");

      assertEquals(2, res.size());
      assertEquals("value1", res.get("a"));
      assertEquals("value2", res.get("b"));
   }

   @Test
   public void testPrefixSufix() {
      Map<String, String> res = resolveVariables("prefix_{variable1}_{variable2}_suffix", "prefix_value1_value2_suffix");

      assertEquals(2, res.size());
      assertEquals("value1", res.get("variable1"));
      assertEquals("value2", res.get("variable2"));
   }

   @Test
   public void testSingleVariableWithPrefix() {
      Map<String, String> res = resolveVariables("counter-{id}", "counter-2345");

      assertEquals(1, res.size());
      assertEquals("2345", res.get("id"));
   }

   @Test
   public void testNull() {
      Map<String, String> res1 = resolveVariables(null, "whatever");
      Map<String, String> res2 = resolveVariables("{hello}", null);

      assertEquals(0, res1.size());
      assertEquals(0, res2.size());
   }

   @Test
   public void testNonConformantPath() {
      Map<String, String> res = resolveVariables("{cachemanager}-{cache}", "default");
      assertEquals(0, res.size());
   }

   @Test
   public void testMalformedExpression() {
      Map<String, String> res = resolveVariables("{counter {id}}", "whatever");

      assertEquals(0, res.size());
   }

   @Test
   public void testMalformedExpression2() {
      Map<String, String> res = resolveVariables("{counter }id}-", "whatever");

      assertEquals(0, res.size());
   }
}
