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
package org.infinispan.distribution.virtualnodes;

import org.infinispan.distribution.DistSyncFuncTest;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional", testName = "distribution.VNodesChFunctionalTest")
public class VNodesChFunctionalTest extends DistSyncFuncTest {
   
   public VNodesChFunctionalTest() {
      numVirtualNodes = 10;
   }

   public void testHashesInitiated() {
      DefaultConsistentHash hash = (DefaultConsistentHash) advancedCache(0, cacheName).getDistributionManager().getConsistentHash();
      containsAllHashes(hash);
      containsAllHashes((DefaultConsistentHash) advancedCache(1, cacheName).getDistributionManager().getConsistentHash());
      containsAllHashes((DefaultConsistentHash) advancedCache(2, cacheName).getDistributionManager().getConsistentHash());
      containsAllHashes((DefaultConsistentHash) advancedCache(3, cacheName).getDistributionManager().getConsistentHash());
   }

   private void containsAllHashes(DefaultConsistentHash ch) {
      assert ch.getCaches().contains(address(0));
      assert ch.getCaches().contains(address(1));
      assert ch.getCaches().contains(address(2));
      assert ch.getCaches().contains(address(3));
   }
}
