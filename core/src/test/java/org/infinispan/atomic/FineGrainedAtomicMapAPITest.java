/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
import org.testng.annotations.Test;

import java.util.Map;

import static org.infinispan.atomic.AtomicMapLookup.getAtomicMap;
import static org.infinispan.atomic.AtomicMapLookup.getFineGrainedAtomicMap;

/**
 * @author Vladimir Blagojevic (C) 2011 Red Hat Inc.
 * @author Sanne Grinovero (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "atomic.FineGrainedAtomicMapAPITest")
public class FineGrainedAtomicMapAPITest extends BaseAtomicHashMapAPITest {

   @SuppressWarnings("UnusedDeclaration")
   @Test(expectedExceptions = {IllegalArgumentException.class})
   public void testFineGrainedMapAfterAtomicMap() throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");

      AtomicMap<String, String> map = getAtomicMap(cache1, "testReplicationRemoveCommit");
      FineGrainedAtomicMap<String, String> map2 = getFineGrainedAtomicMap(cache1, "testReplicationRemoveCommit");
   }


   @Override
   protected <CK, K, V> Map<K, V> createAtomicMap(Cache<CK, Object> cache, CK key, boolean createIfAbsent) {
      return getFineGrainedAtomicMap(cache, key, createIfAbsent);
   }
}
