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
package org.infinispan.util;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash2;
import org.infinispan.commons.hash.MurmurHash2Compat;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(testName = "util.HashFunctionTest", groups = "unit")
public class HashFunctionTest extends AbstractInfinispanTest {

   public void testMurmurHash2Consistency() {
      testHashConsistency(new MurmurHash2());
   }

   public void testMurmurHash2CompatConsistency() {
      testHashConsistency(new MurmurHash2Compat());
   }

   public void testMurmurHash3Consistency() {
      testHashConsistency(new MurmurHash3());
   }

   private void testHashConsistency(Hash hash) {
      Object o = new Object();
      int i1 = hash.hash(o);
      int i2 = hash.hash(o);
      int i3 = hash.hash(o);

      assert i1 == i2: "i1 and i2 are not the same: " + i1 + ", " + i2;
      assert i3 == i2: "i3 and i2 are not the same: " + i2 + ", " + i3;
      assert i1 == i3: "i1 and i3 are not the same: " + i1 + ", " + i3;
   }

}
