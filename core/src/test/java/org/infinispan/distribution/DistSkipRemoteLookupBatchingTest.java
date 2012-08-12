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

package org.infinispan.distribution;

import org.infinispan.test.AbstractCacheTest;
import org.testng.annotations.Test;

import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "distribution.DistSkipRemoteLookupBatchingTest")
public class DistSkipRemoteLookupBatchingTest extends BaseDistFunctionalTest {

   public DistSkipRemoteLookupBatchingTest() {
      cleanup = AbstractCacheTest.CleanupPhase.AFTER_METHOD;
      batchingEnabled = true;
      tx = true;
   }

   public void testSkipLookupOnGetWhileBatching() {
      MagicKey k1 = new MagicKey(c1, c2);
      c1.put(k1, "batchingMagicValue-h1");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      c4.startBatch();
      assert c4.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k1) == null;
      c4.endBatch(true);

      assertOwnershipAndNonOwnership(k1, false);
   }
}
