package org.infinispan.marshall.protostream.impl.adapters;

import static org.testng.AssertJUnit.assertEquals;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "adapters.MapAdapterTest")
public class MapAdapterTest extends AbstractAdapterTest {

   public void testHashMap() throws Exception {
      Map<String, String> original = new HashMap<>();
      original.put("key1", "value1");
      original.put("key2", "value2");
      Map<String, String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testEmptyMap() throws Exception {
      Map<String, String> original = Collections.emptyMap();
      Map<String, String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testSingletonMap() throws Exception {
      Map<String, String> original = Collections.singletonMap("key", "value");
      Map<String, String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testImmutableMap1() throws Exception {
      Map<Integer, Integer> original = Map.of(1, 1);
      Map<Integer, Integer> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testImmutableMapN() throws Exception {
      Map<Integer, Integer> original = Map.of(1, 1, 2, 2, 3, 3);
      Map<Integer, Integer> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }
}
