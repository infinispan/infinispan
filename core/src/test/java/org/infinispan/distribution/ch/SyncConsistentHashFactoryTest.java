/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.distribution.ch;

import java.util.Collection;

import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

/**
 * Test the even distribution and number of moved segments after rebalance for {@link SyncConsistentHashFactory}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "ch.SyncConsistentHashFactoryTest")
public class SyncConsistentHashFactoryTest extends DefaultConsistentHashFactoryTest {
   @Override
   protected ConsistentHashFactory createConsistentHashFactory() {
      return new SyncConsistentHashFactory();
   }

   // Disclaimer: These numbers just happen to work with our test addresses, they are by no means guaranteed
   // by the SyncConsistentHashFactory algorithm. In theory it could trade stability of segments on join/leave
   // in order to guarantee a better distribution, but I haven't done anything in that area yet.
   @Override
   protected int minPrimaryOwned(int numSegments, int numNodes) {
      return (int) (0.25 * super.minPrimaryOwned(numSegments, numNodes));
   }

   @Override
   protected int maxPrimaryOwned(int numSegments, int numNodes) {
      return (int) Math.ceil(3 * super.maxPrimaryOwned(numSegments, numNodes));
   }

   @Override
   protected int minOwned(int numSegments, int numNodes, int actualNumOwners) {
      return (int) (0.25 * super.minOwned(numSegments, numNodes, actualNumOwners));
   }

   @Override
   protected int maxOwned(int numSegments, int numNodes, int actualNumOwners) {
      return (int) Math.ceil(3 * super.maxOwned(numSegments, numNodes, actualNumOwners));
   }

   @Override
   protected int allowedMoves(int numSegments, int numOwners, Collection<Address> oldMembers,
                                 Collection<Address> newMembers) {
      int minMembers = Math.min(oldMembers.size(), newMembers.size());
      int diffMembers = symmetricalDiff(oldMembers, newMembers).size();
      return numSegments * numOwners * (diffMembers / minMembers + 1);
   }
}
