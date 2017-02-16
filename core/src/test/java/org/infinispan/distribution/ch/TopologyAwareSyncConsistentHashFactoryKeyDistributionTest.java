package org.infinispan.distribution.ch;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsTopologyAwareAddress;
import org.jgroups.util.ExtendedUUID;
import org.testng.annotations.Test;

/**
 * Tests the uniformity of the SyncConsistentHashFactory algorithm, which is very similar to the 5.1
 * default consistent hash algorithm virtual nodes.
 *
 * <p>This test assumes that key hashes are random and follow a uniform distribution  so a key has the same chance
 * to land on each one of the 2^31 positions on the hash wheel.
 *
 * <p>The output should stay pretty much the same between runs, so I added and example output here: vnodes_key_dist.txt.
 *
 * <p>Notes about the test output:
 * <ul>
 * <li>{@code P(p)} is the probability of proposition {@code p} being true
 * <li>In the "Primary" rows {@code mean == total_keys / num_nodes} (each key has only one primary owner),
 * but in the "Any owner" rows {@code mean == total_keys / num_nodes * num_owners} (each key is stored on
 * {@code num_owner} nodes).
 * </ul>
 * @author Dan Berindei
 * @since 5.2
 */
@Test(testName = "distribution.ch.TopologyAwareSyncConsistentHashFactoryKeyDistributionTest", groups = "profiling")
public class TopologyAwareSyncConsistentHashFactoryKeyDistributionTest extends SyncConsistentHashFactoryKeyDistributionTest {

   @Override
   protected DefaultConsistentHash createConsistentHash(int numSegments, int numOwners, List<Address> members) {
      MurmurHash3 hash = MurmurHash3.getInstance();
      ConsistentHashFactory<DefaultConsistentHash> chf = new TopologyAwareSyncConsistentHashFactory();
      DefaultConsistentHash ch = chf.create(hash, numOwners, numSegments, members, null);
      return ch;
   }

   @Override
   protected List<Address> createAddresses(int numNodes) {
      ArrayList<Address> addresses = new ArrayList<Address>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         ExtendedUUID topologyAddress = JGroupsTopologyAwareAddress.randomUUID(null, "s" + (i % 2), null, "m" + i);
         addresses.add(new IndexedTopologyAwareJGroupsAddress(topologyAddress, i));
      }
      return addresses;
   }

}

/**
 * We extend JGroupsAddress to make mapping an address to a node easier.
 */
class IndexedTopologyAwareJGroupsAddress extends JGroupsTopologyAwareAddress {
   final int nodeIndex;

   public IndexedTopologyAwareJGroupsAddress(ExtendedUUID address, int nodeIndex) {
      super(address);
      this.nodeIndex = nodeIndex;
   }
}
