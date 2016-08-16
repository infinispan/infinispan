package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.util.InfinispanCollections;
import org.testng.annotations.Test;

/**
 * Tests for the {@link InfinispanCollections} helpers.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "unit", testName = "util.InfinispanCollectionsTest")
public class InfinispanCollectionsTest {

   public void testDifferenceNotStored() {
      Set<String> store = new HashSet<String>();
      store.add("a");
      store.add("b");
      store.add("c");

      Set<String> expected = new HashSet<String>();
      expected.add("a");
      expected.add("b");
      expected.add("c");
      expected.add("d");

      Set<String> notStored = InfinispanCollections.difference(expected, store);
      assertEquals(1, notStored.size());
      assertTrue(notStored.contains("d"));

      Set<String> notRemoved = InfinispanCollections.difference(store, expected);
      assertEquals(0, notRemoved.size());
   }

   public void testDifferenceNotRemoved() {
      Set<String> store = new HashSet<String>();
      store.add("a");
      store.add("b");
      store.add("c");
      store.add("d");

      Set<String> expected = new HashSet<String>();
      expected.add("a");
      expected.add("b");
      expected.add("c");

      Set<String> notStored = InfinispanCollections.difference(expected, store);
      assertEquals(0, notStored.size());

      Set<String> notRemoved = InfinispanCollections.difference(store, expected);
      assertEquals(1, notRemoved.size());
      assertTrue(notRemoved.contains("d"));
   }

   public void testDifferenceNotStoreAndNotRemoved() {
      Set<String> store = new HashSet<String>();
      store.add("a");
      store.add("b");
      store.add("c");
      store.add("d");

      Set<String> expected = new HashSet<String>();
      expected.add("a");
      expected.add("b");
      expected.add("c");
      expected.add("e");

      Set<String> notStored = InfinispanCollections.difference(expected, store);
      assertEquals(1, notStored.size());
      assertTrue(notStored.contains("e"));

      Set<String> notRemoved = InfinispanCollections.difference(store, expected);
      assertEquals(1, notRemoved.size());
      assertTrue(notRemoved.contains("d"));
   }

   public void testNoDifference() {
      Set<String> store = new HashSet<String>();
      store.add("a");
      store.add("b");
      store.add("c");

      Set<String> expected = new HashSet<String>();
      expected.add("a");
      expected.add("b");
      expected.add("c");

      Set<String> notStored = InfinispanCollections.difference(expected, store);
      assertEquals(0, notStored.size());

      Set<String> notRemoved = InfinispanCollections.difference(store, expected);
      assertEquals(0, notRemoved.size());
   }
}
