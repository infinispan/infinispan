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

import org.infinispan.Cache;
import org.infinispan.test.AbstractInfinispanTest;

import java.util.Map;

public class AtomicHashMapTestAssertions extends AbstractInfinispanTest {

   public static void assertIsEmpty(Map map) {
      assert map.isEmpty();
      assert map.get("blah") == null;
      assert !map.containsKey("blah");
   }

   public static <K> void assertIsEmptyMap(Cache<K, ?> cache, K key) {
      assertIsEmpty(AtomicMapLookup.getAtomicMap(cache, key));
   }
}
