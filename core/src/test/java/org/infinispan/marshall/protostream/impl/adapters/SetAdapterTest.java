package org.infinispan.marshall.protostream.impl.adapters;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "adapters.SetAdapterTest")
public class SetAdapterTest extends AbstractAdapterTest {

   public void testEmptySet() throws Exception {
      Set<String> original = Collections.emptySet();
      Set<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testSingletonSet() throws Exception {
      Set<String> original = Collections.singleton("lonely");
      Set<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testSynchronizedSet() throws Exception {
      Set<String> original = Collections.synchronizedSet(new HashSet<>(Set.of("a", "b")));
      Set<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testUnmodifiableSet() throws Exception {
      Set<String> original = Collections.unmodifiableSet(new HashSet<>(Set.of("a", "b")));
      Set<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testImmutableSetN() throws Exception {
      Set<Integer> original = Set.of(1, 2, 3, 4, 5);
      Set<Integer> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testImmutableSet1() throws Exception {
      Set<Integer> original = Set.of(1);
      Set<Integer> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }
}
