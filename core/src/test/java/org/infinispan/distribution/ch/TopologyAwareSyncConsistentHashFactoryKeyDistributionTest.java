package org.infinispan.distribution.ch;

import java.util.List;

import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
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

   protected DefaultConsistentHash createConsistentHash(int numSegments, int numOwners, List<Address> members) {
      ConsistentHashFactory<DefaultConsistentHash> chf = createFactory();
      return chf.create(numOwners, numSegments, members, null);
   }

   @Override
   protected ConsistentHashFactory<DefaultConsistentHash> createFactory() {
      return TopologyAwareSyncConsistentHashFactory.getInstance();
   }

   @Override
   protected Address createSingleAddress(int nodeIndex) {
      return JGroupsAddress.random(null, "s" + (nodeIndex % 2), null, "m" + nodeIndex);
   }
}
