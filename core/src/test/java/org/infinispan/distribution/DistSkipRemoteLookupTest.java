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
package org.infinispan.distribution;

import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;
import java.util.concurrent.ExecutionException;
import org.infinispan.context.Flag;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSkipRemoteLookupTest")
public class DistSkipRemoteLookupTest extends BaseDistFunctionalTest {

   public DistSkipRemoteLookupTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testSkipLookupOnGet() {
      MagicKey k1 = new MagicKey(c1, c2);
      c1.put(k1, "value");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert c4.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k1) == null;

      assertOwnershipAndNonOwnership(k1, false);
   }
   
   public void testCorrectFunctionalityOnConditionalWrite() {
      MagicKey k1 = new MagicKey(c1, c2);
      c1.put(k1, "value");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert c4.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).putIfAbsent(k1, "new_val") == null;

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      if (l1CacheEnabled) assertIsNotInL1(c4, k1);
   }

   public void testCorrectFunctionalityOnUnconditionalWrite() {
      MagicKey k1 = new MagicKey(c1, c2);
      c1.put(k1, "value");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert c4.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).put(k1, "new_val") == null;
      assert c3.get(k1).equals("new_val");
      assertOnAllCachesAndOwnership(k1, "new_val");
   }
   
   @Test
   public void testSkipLookupOnRemove() {
      MagicKey k1 = new MagicKey(c1, c2);
      final String value = "SomethingToSayHere";

      assert null == c1.put(k1, value);
      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);
      
      assert value.equals(c1.get(k1));
      assert value.equals(c1.remove(k1));
      assert null == c1.put(k1, value);

      assertIsNotInL1(c3, k1);
      assert value.equals(c3.remove(k1));
      assert null == c1.put(k1, value);

      assert null == c4.getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).remove(k1);
   }
   
   @Test
   public void testSkipLookupOnAsyncRemove() throws InterruptedException, ExecutionException {
      MagicKey k1 = new MagicKey(c1, c2);
      final String value = "SomethingToSayHere-async";

      assert null == c1.put(k1, value);
      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);
      
      assert value.equals(c1.get(k1));
      assert value.equals(c1.remove(k1));
      assert null == c1.put(k1, value);

      assertIsNotInL1(c3, k1);
      assert value.equals(c3.remove(k1));
      assert null == c1.put(k1, value);

      assert null == c4.getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).removeAsync(k1).get();
   }

}
