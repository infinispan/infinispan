package org.infinispan.marshall.exts;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "marshall.CollectionExternalizerTest")
public class CollectionExternalizerTest extends AbstractExternalizerTest<Collection> {

   @Override
   AdvancedExternalizer<Collection> createExternalizer() {
      return new CollectionExternalizer();
   }

   public void abstractListSubListTest() throws Exception {
      List<String> list = Arrays.asList("a", "b", "c", "d", "e").subList(2, 3);
      Collection deserialized = deserialize(list);
      assertEquals(list, deserialized);
   }

   public void arrayListSubListTest() throws Exception {
      List<String> list = new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e")).subList(2, 3);
      Collection deserialized = deserialize(list);
      assertEquals(list, deserialized);
   }

   public void listOf12() throws Exception {
      List<String> list = List.of("a", "b");
      Collection deserialized = deserialize(list);
      assertEquals(list, deserialized);
   }

   public void listOfN() throws Exception {
      List<String> list = List.of("a", "b", "c");
      Collection deserialized = deserialize(list);
      assertEquals(list, deserialized);
   }

   public void singletonList() throws Exception {
      List<String> list = Collections.singletonList("a");
      Collection deserialized = deserialize(list);
      assertEquals(list, deserialized);
   }

   public void unmodifiableList() throws Exception {
      List<String> list = Collections.unmodifiableList(new ArrayList<>());
      Collection deserialized = deserialize(list);
      assertEquals(list, deserialized);
   }

   public void emptyList() throws Exception {
      List<Object> list = Collections.emptyList();
      Collection deserialized = deserialize(list);
      assertEquals(list, deserialized);
   }

   public void synchronizedList() throws Exception {
      List<String> list = Collections.synchronizedList(new ArrayList<>());
      Collection deserialized = deserialize(list);
      assertEquals(list, deserialized);
   }

   public void singletonSet() throws Exception {
      Set<String> set = Collections.singleton("a");
      Collection deserialized = deserialize(set);
      assertEquals(set, deserialized);
   }

   public void synchronizedSet() throws Exception {
      Set<Object> set = Collections.synchronizedSet(new HashSet<>());
      Collection deserialized = deserialize(set);
      assertEquals(set, deserialized);
   }

   public void emptySet() throws Exception {
      Set<Object> set = Collections.emptySet();
      Collection deserialized = deserialize(set);
      assertEquals(set, deserialized);
   }

   public void unmodifiableSet() throws Exception {
      Set<Object> list = Collections.unmodifiableSet(new HashSet<>());
      Collection deserialized = deserialize(list);
      assertEquals(list, deserialized);
   }
}
