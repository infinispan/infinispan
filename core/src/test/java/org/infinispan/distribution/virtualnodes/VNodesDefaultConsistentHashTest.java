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

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.oldch.ConsistentHashHelper;
import org.infinispan.distribution.oldch.DefaultConsistentHash;
import org.infinispan.distribution.oldch.TopologyAwareConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;

@Test(groups = "unit", testName = "distribution.VNodesDefaultConsistentHashTest", enabled = true)
public class VNodesDefaultConsistentHashTest extends AbstractInfinispanTest {

   public DefaultConsistentHash createConsistentHash(List<Address> servers, int numVirtualNodes) {
      DefaultConsistentHash ch = new DefaultConsistentHash(new MurmurHash3());
      ch.setCaches(new HashSet<Address>(servers));
      return ch;
   }

   public void testEveryNumOwners() {
      int[] numVirtualNodesList = {2, 10, 100};
      for (int numVirtualNodes : numVirtualNodesList) {
         for (int nodesCount = 1; nodesCount < 10; nodesCount++) {
            ArrayList servers = new ArrayList(nodesCount);
            for (int i = 0; i < nodesCount; i++) {
               servers.add(new TestAddress(i * 1000));
            }

            DefaultConsistentHash ch = createConsistentHash(servers, numVirtualNodes);
            List<Address> sortedServers = new ArrayList<Address>(ch.getCaches());

            // check that we get numOwners servers for numOwners in 1..nodesCount
            for (int numOwners = 1; numOwners < nodesCount; numOwners++) {
               for (int i = 0; i < nodesCount; i++) {
                  List<Address> owners = ch.locate(sortedServers.get(i), numOwners);
                  assertEquals(owners.size(), numOwners);
                  assertEquals(owners.get(0), sortedServers.get(i));
                  assertEquals(new HashSet<Address>(owners).size(), numOwners);
               }
            }

            // check that we get all the servers for numOwners > nodesCount
            for (int i = 0; i < nodesCount; i++) {
               List<Address> owners = ch.locate(sortedServers.get(i), nodesCount + 1);
               assertEqualsNoOrder(owners.toArray(), servers.toArray());
            }
         }
      }
   }
}
