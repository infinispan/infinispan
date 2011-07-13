/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.atomic;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "atomic.AtomicHashMapTest")
public class AtomicHashMapTest extends AbstractInfinispanTest {
   public void testDeltasWithEmptyMap() {
      AtomicHashMap m = new AtomicHashMap();
      Delta d = m.delta();
      assert d instanceof NullDelta;

      AtomicHashMap newMap = new AtomicHashMap();
      newMap.initForWriting();
      newMap.put("k", "v");
      newMap = (AtomicHashMap) d.merge(newMap);
      assert newMap.containsKey("k");
      assert newMap.size() == 1;

      newMap = (AtomicHashMap) d.merge(null);
      assert newMap.isEmpty();
   }

   public void testDeltasWithNoChanges() {
      AtomicHashMap m = new AtomicHashMap();
      m.initForWriting();
      m.put("k1", "v1");
      m.commit();
      assert m.size() == 1;
      Delta d = m.delta();
      assert d instanceof NullDelta;

      AtomicHashMap newMap = new AtomicHashMap();
      newMap.initForWriting();
      newMap.put("k", "v");
      newMap = (AtomicHashMap) d.merge(newMap);
      assert newMap.containsKey("k");
      assert newMap.size() == 1;

      newMap = (AtomicHashMap) d.merge(null);
      assert newMap.isEmpty();
   }

   public void testDeltasWithRepeatedChanges() {
      AtomicHashMap m = new AtomicHashMap();
      m.initForWriting();
      m.put("k1", "v1");
      m.put("k1", "v2");
      m.put("k1", "v3");
      assert m.size() == 1;
      AtomicHashMapDelta d = (AtomicHashMapDelta) m.delta();
      assert d.getChangeLogSize() != 0;

      AtomicHashMap newMap = new AtomicHashMap();
      newMap.initForWriting();
      newMap.put("k1", "v4");
      newMap = (AtomicHashMap) d.merge(newMap);
      assert newMap.containsKey("k1");
      assert newMap.get("k1").equals("v3");
      assert newMap.size() == 1;

      newMap = (AtomicHashMap) d.merge(null);
      assert newMap.containsKey("k1");
      assert newMap.get("k1").equals("v3");
      assert newMap.size() == 1;
   }
}
