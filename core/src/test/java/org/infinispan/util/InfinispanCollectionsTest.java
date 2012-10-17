/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.util;

import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(testName = "util.InfinispanCollectionsTest")
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

   public void testEmptyCollectionsIteratorsSame() {
      assertTrue(InfinispanCollections.emptySet().iterator()
            == InfinispanCollections.emptySet().iterator());

      assertTrue(InfinispanCollections.emptyMap().keySet().iterator()
            == InfinispanCollections.emptyMap().keySet().iterator());
      assertTrue(InfinispanCollections.emptyMap().values().iterator()
            == InfinispanCollections.emptyMap().values().iterator());
      assertTrue(InfinispanCollections.emptyMap().entrySet().iterator()
            == InfinispanCollections.emptyMap().entrySet().iterator());

      assertTrue(InfinispanCollections.emptyList().iterator()
            == InfinispanCollections.emptyList().iterator());
   }

}
