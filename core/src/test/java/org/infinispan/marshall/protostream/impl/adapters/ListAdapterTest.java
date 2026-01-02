package org.infinispan.marshall.protostream.impl.adapters;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "adapters.ListAdapterTest")
public class ListAdapterTest extends AbstractAdapterTest {

   public void testArraysAsList() throws Exception {
      List<String> original = Arrays.asList("one", "two", "three");
      List<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testSubList() throws Exception {
      List<String> original = new ArrayList<>(Arrays.asList("one", "two", "three", "four")).subList(1, 3);
      List<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testEmptyList() throws Exception {
      List<String> original = Collections.emptyList();
      List<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testSingletonList() throws Exception {
      List<String> original = Collections.singletonList("lonely");
      List<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testSynchronizedList() throws Exception {
      List<String> original = Collections.synchronizedList(new ArrayList<>(Arrays.asList("a", "b")));
      List<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testUnmodifiableList() throws Exception {
      List<String> original = Collections.unmodifiableList(new ArrayList<>(Arrays.asList("a", "b")));
      List<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testImmutableListN() throws Exception {
      List<Integer> original = List.of(1, 2, 3, 4, 5);
      List<Integer> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testImmutableList1() throws Exception {
      List<Integer> original = List.of(1);
      List<Integer> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }
}
